use crate::agent::{Activation, Agent};
use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{sleep, Instant};
use tracing::{debug, error, info, warn};

/// Circuit breaker state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CircuitState {
    Closed,   // Normal operation
    Open,     // Failing, reject requests
    HalfOpen, // Testing if service recovered
}

/// Circuit breaker for agent execution
pub struct CircuitBreaker {
    state: Arc<tokio::sync::RwLock<CircuitState>>,
    failure_count: Arc<tokio::sync::RwLock<u32>>,
    last_failure_time: Arc<tokio::sync::RwLock<Option<Instant>>>,
    failure_threshold: u32,
    timeout: Duration,
    half_open_timeout: Duration,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, timeout: Duration) -> Self {
        Self {
            state: Arc::new(tokio::sync::RwLock::new(CircuitState::Closed)),
            failure_count: Arc::new(tokio::sync::RwLock::new(0)),
            last_failure_time: Arc::new(tokio::sync::RwLock::new(None)),
            failure_threshold,
            timeout,
            half_open_timeout: Duration::from_secs(60), // Try again after 60s
        }
    }

    pub async fn call<F, Fut>(&self, f: F) -> Result<()>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let state = *self.state.read().await;

        match state {
            CircuitState::Open => {
                let last_failure = *self.last_failure_time.read().await;
                if let Some(last) = last_failure {
                    if last.elapsed() > self.half_open_timeout {
                        // Transition to half-open
                        *self.state.write().await = CircuitState::HalfOpen;
                        *self.failure_count.write().await = 0;
                        debug!("Circuit breaker transitioning to half-open");
                    } else {
                        return Err(anyhow::anyhow!("Circuit breaker is open"));
                    }
                } else {
                    return Err(anyhow::anyhow!("Circuit breaker is open"));
                }
            }
            CircuitState::HalfOpen => {
                // Allow one attempt
            }
            CircuitState::Closed => {
                // Normal operation
            }
        }

        match f().await {
            Ok(()) => {
                // Success - reset failure count
                *self.failure_count.write().await = 0;
                if state == CircuitState::HalfOpen {
                    *self.state.write().await = CircuitState::Closed;
                    info!("Circuit breaker closed - service recovered");
                }
                Ok(())
            }
            Err(e) => {
                // Failure - increment count
                let mut count = self.failure_count.write().await;
                *count += 1;
                *self.last_failure_time.write().await = Some(Instant::now());

                if *count >= self.failure_threshold {
                    *self.state.write().await = CircuitState::Open;
                    warn!("Circuit breaker opened after {} failures", *count);
                }
                Err(e)
            }
        }
    }
}

/// Agent execution task with retry
pub struct AgentTask {
    pub activation: Activation,
    pub agent: Arc<dyn Agent>,
    pub max_retries: u32,
    pub circuit_breaker: Option<Arc<CircuitBreaker>>,
    pub dlq_sender: Option<mpsc::UnboundedSender<Activation>>,
}

impl AgentTask {
    pub fn new(
        activation: Activation,
        agent: Arc<dyn Agent>,
        max_retries: u32,
        circuit_breaker: Option<Arc<CircuitBreaker>>,
        dlq_sender: Option<mpsc::UnboundedSender<Activation>>,
    ) -> Self {
        Self {
            activation,
            agent,
            max_retries,
            circuit_breaker,
            dlq_sender,
        }
    }

    async fn execute_with_retry(&self) -> Result<()> {
        let mut last_error = None;

        for attempt in 0..=self.max_retries {
            if attempt > 0 {
                // Exponential backoff: 1s, 2s, 4s, 8s, ...
                let delay = Duration::from_secs(2_u64.pow(attempt - 1));
                debug!(
                    rule_id = %self.activation.rule_id,
                    attempt = attempt,
                    delay_secs = delay.as_secs(),
                    "Retrying agent execution"
                );
                sleep(delay).await;
            }

            let result = if let Some(cb) = &self.circuit_breaker {
                cb.call(|| self.agent.execute(&self.activation)).await
            } else {
                self.agent.execute(&self.activation).await
            };

            match result {
                Ok(()) => {
                    if attempt > 0 {
                        info!(
                            rule_id = %self.activation.rule_id,
                            attempt = attempt,
                            "Agent execution succeeded after retry"
                        );
                    }
                    return Ok(());
                }
                Err(e) => {
                    last_error = Some(e);
                    if attempt < self.max_retries {
                        warn!(
                            rule_id = %self.activation.rule_id,
                            attempt = attempt,
                            "Agent execution failed, will retry"
                        );
                    }
                }
            }
        }

        error!(
            rule_id = %self.activation.rule_id,
            max_retries = self.max_retries,
            "Agent execution failed after all retries"
        );
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Unknown error")))
    }
}

/// Background agent execution queue with backpressure support
pub struct AgentQueue {
    sender: mpsc::Sender<AgentTask>, // Bounded channel for backpressure
    pub dlq_sender: mpsc::UnboundedSender<Activation>, // Dead letter queue
}

impl AgentQueue {
    pub fn new(capacity: Option<usize>) -> (Self, AgentQueueWorker) {
        let capacity = capacity.unwrap_or(1000);
        let (tx, rx) = mpsc::channel(capacity); // Bounded channel for backpressure
        let (dlq_tx, dlq_rx) = mpsc::unbounded_channel();

        let worker = AgentQueueWorker {
            receiver: rx,
            dlq_receiver: Some(dlq_rx),
            concurrency: 1, // Default, will be set via with_concurrency
        };

        (
            Self {
                sender: tx,
                dlq_sender: dlq_tx,
            },
            worker,
        )
    }

    pub async fn enqueue(&self, task: AgentTask) -> Result<()> {
        // This will block if the queue is full (backpressure)
        self.sender
            .send(task)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to enqueue agent task: {}", e))?;
        Ok(())
    }

    pub fn try_enqueue(&self, task: AgentTask) -> Result<()> {
        // Non-blocking version - returns error if queue is full
        self.sender
            .try_send(task)
            .map_err(|e| anyhow::anyhow!("Agent queue full (backpressure): {}", e))?;
        Ok(())
    }
}

/// Worker that processes agent tasks in the background with configurable concurrency
pub struct AgentQueueWorker {
    receiver: mpsc::Receiver<AgentTask>,
    #[allow(dead_code)]
    dlq_receiver: Option<mpsc::UnboundedReceiver<Activation>>, // Reserved for future DLQ processing
    concurrency: usize,
}

impl AgentQueueWorker {
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }
}

/// Dead letter queue handler
pub struct DeadLetterQueue {
    receiver: mpsc::UnboundedReceiver<Activation>,
}

impl DeadLetterQueue {
    pub fn new(receiver: mpsc::UnboundedReceiver<Activation>) -> Self {
        Self { receiver }
    }

    pub async fn run(mut self) {
        info!("Dead letter queue handler started");
        let mut count = 0;
        while let Some(activation) = self.receiver.recv().await {
            count += 1;
            warn!(
                rule_id = %activation.rule_id,
                rule_name = %activation.rule_name,
                count = count,
                "Activation sent to dead letter queue"
            );
            // In production, you'd store this to persistent storage
        }
        info!("Dead letter queue handler stopped");
    }
}

impl AgentQueueWorker {
    pub async fn run(mut self) {
        let concurrency = self.concurrency.max(1); // At least 1 worker
        info!(concurrency = concurrency, "Agent queue worker started");

        // Spawn concurrent workers for parallel agent execution
        // Use a shared receiver with Arc<Mutex> for concurrent access
        let (task_tx, task_rx) = mpsc::unbounded_channel::<AgentTask>();
        let shared_rx = Arc::new(tokio::sync::Mutex::new(task_rx));

        // Spawn worker tasks
        let mut worker_handles = Vec::new();
        for worker_id in 0..concurrency {
            let rx = Arc::clone(&shared_rx);
            let handle = tokio::spawn(async move {
                loop {
                    let task = {
                        let mut rx_guard = rx.lock().await;
                        rx_guard.recv().await
                    };
                    let task = match task {
                        Some(t) => t,
                        None => break, // Channel closed
                    };
                    let start = Instant::now();
                    match task.execute_with_retry().await {
                        Ok(()) => {
                            let duration = start.elapsed();
                            debug!(
                                worker_id = worker_id,
                                rule_id = %task.activation.rule_id,
                                agent = task.agent.name(),
                                duration_ms = duration.as_millis(),
                                "Agent execution completed"
                            );
                            // Record success metric
                            crate::metrics::METRICS.record_agent_execution_duration(
                                task.agent.name(),
                                duration.as_secs_f64(),
                            );
                        }
                        Err(e) => {
                            error!(
                                worker_id = worker_id,
                                rule_id = %task.activation.rule_id,
                                error = %e,
                                "Agent execution failed permanently"
                            );
                            // Send to dead letter queue
                            if let Some(dlq) = &task.dlq_sender {
                                if let Err(dlq_err) = dlq.send(task.activation.clone()) {
                                    warn!(
                                        rule_id = %task.activation.rule_id,
                                        error = %dlq_err,
                                        "Failed to send to DLQ"
                                    );
                                }
                            }
                            crate::metrics::METRICS
                                .agent_failures
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            });
            worker_handles.push(handle);
        }

        // Main loop: receive tasks and distribute to workers
        while let Some(task) = self.receiver.recv().await {
            if let Err(e) = task_tx.send(task) {
                error!("Failed to distribute task to worker: {}", e);
                break;
            }
        }

        // Close the task channel and wait for workers
        drop(task_tx);
        for handle in worker_handles {
            let _ = handle.await;
        }

        info!("Agent queue worker stopped");
    }
}
