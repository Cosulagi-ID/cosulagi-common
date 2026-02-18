# Cosulagi Common Library

## Overview
Shared Go library serving as the foundation for all Cosulagi backend microservices. Provides standardized implementations for logging, messaging, and RPC communication.

## Structure
- `logger/`: **Logging Infrastructure**
  - Wrapper around `rs/zerolog`.
  - Includes `DiscordHook` for sending critical logs/errors to Discord channels.
  - Supports batching and rate limiting.
- `message/`: **Messaging Layer**
  - Managed RabbitMQ connection and channel handling.
  - Dependencies: `amqp091-go`, `go-amqp-reconnect`.
- `rpc/`: **Remote Procedure Calls**
  - Custom RPC implementation over RabbitMQ (`rpc_queue`).
  - Supports timeout handling and asynchronous request/response patterns.

## Usage
Import into backend services to ensure consistency:

```go
import (
    "github.com/Cosulagi-ID/cosulagi-common/logger"
    "github.com/Cosulagi-ID/cosulagi-common/message"
    "github.com/Cosulagi-ID/cosulagi-common/rpc"
)
```

## Key Dependencies
- **RabbitMQ**: `github.com/rabbitmq/amqp091-go`
- **Logging**: `github.com/rs/zerolog`
- **Config**: `github.com/spf13/viper`

---
[Return to Root AGENTS.md](../../AGENTS.md)
