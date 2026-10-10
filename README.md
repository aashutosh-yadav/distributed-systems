Distributed systems algos , papers and concepts implementation .

will further transform into a meaningfull project.

---

## Simulating Distributed Systems in Go

You can simulate a distributed system at three different levels, ranging from goroutines within a single process to isolated Docker containers.

### Level 1 — Goroutines (Virtual Nodes, Single Process)

Go's goroutines are lightweight concurrent tasks. Each goroutine can act as a virtual node with its own local state, communicating with other goroutines through Go channels instead of a network.

```go
func node(id int, in <-chan string, out chan<- string) {
    // Each goroutine simulates a node within one process.
}

func main() {
    ch := make(chan string)

    go node(1, ch, ch)
    go node(2, ch, ch)
    go node(3, ch, ch)
}
```

**Characteristics:**
- All nodes run within a single operating-system process.
- Goroutines share the process's address space and resources.
- Channels provide communication and synchronization.
- Useful for learning concurrency, message passing, and distributed algorithms.

**Limitation:** These are virtual nodes, not independent machines. A process crash terminates all its goroutines.

### Level 2 — Multiple OS Processes on Localhost

Run the same Go program multiple times, with each instance listening on a different TCP port. Each process has its own memory space and communicates with other processes over the loopback network.

```bash
go run node.go -id=1 -port=8001 &
go run node.go -id=2 -port=8002 &
go run node.go -id=3 -port=8003 &
```

**Characteristics:**
- Each node runs as a separate operating-system process.
- Processes have independent memory spaces.
- Nodes communicate through network protocols such as TCP or HTTP.
- One node can crash without automatically terminating the others.

**Limitation:** All processes still run on the same physical machine and share its operating system and hardware.

### Level 3 — Docker Containers (Isolated Node Environments)

Docker allows each node to run inside its own container, with an isolated filesystem, process environment, and network identity. Docker Compose makes it easier to start and manage the entire cluster.

Example `compose.yaml`:

```yaml
services:
  node1:
    build: .
    environment:
      NODE_ID: "1"

  node2:
    build: .
    environment:
      NODE_ID: "2"
```

Start the cluster:

```bash
docker compose up --build
```

**Characteristics:**
- Each container runs an independent node process.
- Containers provide filesystem and process isolation.
- Nodes can communicate over a Docker network.
- You can configure multiple nodes, network delays, and failures for testing.

You can also scale a service:

```bash
docker compose up --build --scale worker=10
```

For this command, define a `worker` service in your Compose file. Avoid fixed host-port mappings for replicas unless each replica has a distinct port configuration.

**Limitation:** Containers on one laptop still share the host's kernel and physical resources. They simulate many aspects of a distributed deployment but are not equivalent to separate physical machines.

### Comparison

| Feature | Level 1: Goroutines | Level 2: Processes | Level 3: Containers |
|---|---|---|---|
| Separate OS processes | No | Yes | Yes |
| Independent memory spaces | No | Yes | Yes |
| Network communication | No, channels | Yes | Yes |
| Filesystem isolation | No | No | Yes |
| Separate physical machines | No | No | No |
| Setup complexity | Low | Medium | Medium |

### Recommended Learning Path

1. **Goroutines:** Understand concurrency, channels, synchronization, and race conditions.
2. **OS processes:** Implement TCP communication, message serialization, and node failure handling.
3. **Docker:** Run a multi-node cluster and experiment with network failures, retries, and recovery.

This progression helps you move from concurrent programming to distributed systems while gradually introducing the challenges of communication, isolation, and failure handling.
