# MemberList - A Distributed Chatroom Member Management System

## Overview
MemberList is a scalable, Redis-based system for managing online members in a chatroom. It efficiently handles dynamic sharding, user heartbeats, and automatic shard expansion, ensuring smooth operations even under high load.

## Features
- **Dynamic Sharding**: Uses a hash-based sharding mechanism to distribute users across multiple shards.
- **Shard Expansion**: Automatically doubles the number of shards when capacity is exceeded.
- **Dual Write During Expansion**: Ensures consistency by writing to both the new and previous shards.
- **Heartbeat Tracking**: Maintains online status using Redis ZSets with expiration logic.
- **Automatic Cleanup**: Periodically removes inactive users and old shards after expansion.

## Usage
### Running the System
```sh
go run main.go
```

## How It Works
1. **Sharding**: Users are distributed across shards using CRC32 hashing.
2. **Expansion**: If a shard exceeds `MaxShardSize`, the number of shards doubles.
3. **Heartbeat Mechanism**: Users periodically send heartbeats to stay online.
4. **Cleanup Routine**: A background task removes inactive users and old shards.

## Configuration
Modify constants in `main.go` to adjust behavior:
```go
const (
    MaxShardSize      = 100000
    MaxShardCount     = 100
    InitialShardCount = 4
    HeartbeatTimeout  = 120 // seconds
    ExpandingDuration = 300 // seconds
    ExpandFactor      = 2
)
```

## Contributing
1. Fork the repository.
2. Create a feature branch .
3. Commit your changes.
4. Push to the branch.
5. Open a pull request.

## License
This project is licensed under the MIT License.

