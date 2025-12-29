use crate::RuleEngine;
use anyhow::{Context, Result};
use arrow_json::ReaderBuilder;
use std::io::Cursor;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio_tungstenite::WebSocketStream;
use tracing::{debug, error, info};

pub type SharedEngine = Arc<RwLock<RuleEngine>>;

/// Kafka consumer for ingesting events
pub struct KafkaIngestion {
    engine: SharedEngine,
    brokers: Vec<String>,
    topic: String,
    group_id: String,
    auto_commit: bool,
}

impl KafkaIngestion {
    pub fn new(
        engine: SharedEngine,
        brokers: Vec<String>,
        topic: String,
        group_id: String,
        auto_commit: bool,
    ) -> Self {
        Self {
            engine,
            brokers,
            topic,
            group_id,
            auto_commit,
        }
    }

    pub async fn run(&self) -> Result<()> {
        use futures::StreamExt;
        use rdkafka::config::ClientConfig;
        use rdkafka::consumer::{Consumer, StreamConsumer};
        use rdkafka::Message;

        info!(
            brokers = ?self.brokers,
            topic = %self.topic,
            group_id = %self.group_id,
            "Starting Kafka consumer"
        );

        // Create Kafka consumer
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", self.brokers.join(","))
            .set("group.id", &self.group_id)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set(
                "enable.auto.commit",
                if self.auto_commit { "true" } else { "false" },
            )
            .set("auto.offset.reset", "earliest")
            .create()
            .context("Failed to create Kafka consumer")?;

        consumer
            .subscribe(&[&self.topic])
            .context("Failed to subscribe to Kafka topic")?;

        info!("Kafka consumer subscribed to topic: {}", self.topic);

        // Process messages
        let mut message_stream = consumer.stream();
        while let Some(message_result) = message_stream.next().await {
            match message_result {
                Ok(message) => {
                    if let Some(payload) = message.payload() {
                        match self.process_message(payload).await {
                            Ok(_) => {
                                debug!(
                                    partition = ?message.partition(),
                                    offset = ?message.offset(),
                                    "Processed Kafka message"
                                );
                            }
                            Err(e) => {
                                error!(
                                    error = %e,
                                    partition = ?message.partition(),
                                    offset = ?message.offset(),
                                    "Failed to process Kafka message"
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "Kafka message error");
                }
            }
        }

        Ok(())
    }

    async fn process_message(&self, payload: &[u8]) -> Result<()> {
        // Try to parse as JSON
        let json_value: serde_json::Value =
            serde_json::from_slice(payload).context("Failed to parse Kafka message as JSON")?;

        // Convert to RecordBatch
        let json_data = serde_json::to_vec(&json_value)?;
        let cursor = Cursor::new(json_data);

        let engine_lock = self.engine.read().await;
        let schema = engine_lock.schema();
        drop(engine_lock);

        let reader = ReaderBuilder::new(schema.clone())
            .build(cursor)
            .context("Failed to create JSON reader")?;

        // Process all batches from the reader
        for batch_result in reader {
            match batch_result {
                Ok(batch) => {
                    let mut engine_lock = self.engine.write().await;
                    match engine_lock.process_batch(&batch).await {
                        Ok(_traces) => {
                            debug!(rows = batch.num_rows(), "Processed batch from Kafka");
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to process batch from Kafka");
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to read batch from Kafka message");
                }
            }
        }

        Ok(())
    }
}

/// WebSocket server for ingesting events
pub struct WebSocketIngestion {
    engine: SharedEngine,
    bind: String,
    max_connections: usize,
}

impl WebSocketIngestion {
    pub fn new(engine: SharedEngine, bind: String, max_connections: usize) -> Self {
        Self {
            engine,
            bind,
            max_connections,
        }
    }

    pub async fn run(&self) -> Result<()> {
        use tokio::net::TcpListener;
        use tokio_tungstenite::accept_async;

        info!(
            bind = %self.bind,
            max_connections = self.max_connections,
            "Starting WebSocket server"
        );

        let listener = TcpListener::bind(&self.bind).await?;
        info!("WebSocket server listening on {}", self.bind);

        while let Ok((stream, addr)) = listener.accept().await {
            let engine = self.engine.clone();
            tokio::spawn(async move {
                info!(client = %addr, "New WebSocket connection");
                match accept_async(stream).await {
                    Ok(ws_stream) => {
                        if let Err(e) = handle_websocket_stream(ws_stream, engine).await {
                            error!(error = %e, "WebSocket handler error");
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to accept WebSocket connection");
                    }
                }
            });
        }

        Ok(())
    }
}

async fn handle_websocket_stream(
    stream: WebSocketStream<TcpStream>,
    engine: SharedEngine,
) -> Result<()> {
    use arrow_json::ReaderBuilder;
    use futures::{SinkExt, StreamExt};
    use std::io::Cursor;
    use tokio_tungstenite::tungstenite::Message;

    let (mut sender, mut receiver) = stream.split();

    while let Some(msg) = receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                // Try to parse as JSON
                match serde_json::from_str::<serde_json::Value>(&text) {
                    Ok(json_value) => {
                        // Convert to RecordBatch
                        let json_data = match serde_json::to_vec(&json_value) {
                            Ok(data) => data,
                            Err(e) => {
                                error!(error = %e, "Failed to serialize JSON");
                                continue;
                            }
                        };
                        let cursor = Cursor::new(json_data);

                        let engine_lock = engine.read().await;
                        let schema = engine_lock.schema();
                        drop(engine_lock);

                        let reader = match ReaderBuilder::new(schema.clone()).build(cursor) {
                            Ok(r) => r,
                            Err(e) => {
                                error!(error = %e, "Failed to create JSON reader");
                                continue;
                            }
                        };

                        // Process all batches
                        for batch_result in reader {
                            match batch_result {
                                Ok(batch) => {
                                    let mut engine_lock = engine.write().await;
                                    match engine_lock.process_batch(&batch).await {
                                        Ok(_traces) => {
                                            debug!(
                                                rows = batch.num_rows(),
                                                "Processed batch from WebSocket"
                                            );
                                        }
                                        Err(e) => {
                                            error!(error = %e, "Failed to process batch from WebSocket");
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!(error = %e, "Failed to read batch from WebSocket message");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to parse WebSocket message as JSON");
                    }
                }
            }
            Ok(Message::Binary(data)) => {
                // Try to parse binary as JSON
                match serde_json::from_slice::<serde_json::Value>(&data) {
                    Ok(json_value) => {
                        // Same processing as text
                        let json_data = match serde_json::to_vec(&json_value) {
                            Ok(data) => data,
                            Err(e) => {
                                error!(error = %e, "Failed to serialize JSON");
                                continue;
                            }
                        };
                        let cursor = Cursor::new(json_data);

                        let engine_lock = engine.read().await;
                        let schema = engine_lock.schema();
                        drop(engine_lock);

                        let reader = match ReaderBuilder::new(schema.clone()).build(cursor) {
                            Ok(r) => r,
                            Err(e) => {
                                error!(error = %e, "Failed to create JSON reader");
                                continue;
                            }
                        };

                        for batch_result in reader {
                            match batch_result {
                                Ok(batch) => {
                                    let mut engine_lock = engine.write().await;
                                    if let Err(e) = engine_lock.process_batch(&batch).await {
                                        error!(error = %e, "Failed to process batch from WebSocket");
                                    }
                                }
                                Err(e) => {
                                    error!(error = %e, "Failed to read batch from WebSocket message");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to parse WebSocket binary as JSON");
                    }
                }
            }
            Ok(Message::Close(_)) => {
                info!("WebSocket connection closed");
                break;
            }
            Ok(Message::Ping(data)) => {
                // Respond with pong
                if sender.send(Message::Pong(data)).await.is_err() {
                    break;
                }
            }
            Ok(Message::Pong(_)) => {
                // Ignore pong
            }
            Ok(Message::Frame(_)) => {
                // Ignore frames
            }
            Err(e) => {
                error!(error = %e, "WebSocket error");
                break;
            }
        }
    }

    info!("WebSocket connection ended");
    Ok(())
}
