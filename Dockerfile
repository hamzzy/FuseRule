# Build stage
FROM rust:1.75-slim AS builder

WORKDIR /usr/src/fuserule
COPY . .

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
RUN cargo build --release

# Final stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /usr/src/fuserule/target/release/arrow-rule-agent /app/fuserule
COPY fuse_rule_config.yaml /app/fuse_rule_config.yaml

EXPOSE 3030

ENTRYPOINT ["/app/fuserule"]
CMD ["run", "--config", "/app/fuse_rule_config.yaml", "--port", "3030"]
