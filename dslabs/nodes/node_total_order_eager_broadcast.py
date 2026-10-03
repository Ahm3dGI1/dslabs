from dataclasses import dataclass, field
from typing import Any

from dslabs.protocols import Message, Scheduler, Transport


@dataclass
class NodeTotalOrderEagerBroadcast:
    """Eager broadcast with total order, using Lamport clocks.

    Each write carries the clock of the node that made it, so
    (timestamp, origin) orders every write the same way on every node.

    Writes are buffered. The smallest is delivered once every node's clock has
    passed its timestamp (nothing smaller can still arrive); `last_seen` tracks
    that, and messages carry the sender's clock.
    """

    node_id: str
    peers: list[str]
    transport: Transport
    scheduler: Scheduler

    clock: int = 0
    last_seen: dict[str, int] = field(default_factory=dict)
    buffer: dict[tuple[int, str], tuple[str, Any]] = field(default_factory=dict)
    seen: set[tuple[int, str]] = field(default_factory=set)

    # Key-value store
    store: dict[str, Any] = field(default_factory=dict)
    # Append-only log of everything delivered, in delivery order
    log: list[tuple[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.last_seen = {peer: 0 for peer in self.peers}

    # Client-facing API
    def client_put(self, key: str, value: Any) -> None:
        self.clock += 1
        self.last_seen[self.node_id] = self.clock
        self.receive_and_replicate(key, value, self.clock, self.node_id)
        self.deliver_ready()

    def client_get(self, key: str) -> Any:
        return self.store.get(key)

    # Node internals
    def receive_and_replicate(self, key: str, value: Any, ts: int, origin: str) -> None:
        """Buffer a write the first time we see it (whether from ourselves or others) and pass it to other peers"""
        if (ts, origin) in self.seen:
            return
        self.seen.add((ts, origin))
        self.buffer[(ts, origin)] = (key, value)
        for peer in self.peers:
            if peer != self.node_id:
                self.transport.send(peer, {"type": "replicate", "key": key, "value": value,
                                           "ts": ts, "origin": origin, "sender_clock": self.clock})

    def deliver_ready(self) -> None:
        """Deliver in timestamp order when ready"""
        while self.buffer:
            at = min(self.buffer)
            if at[0] > min(self.last_seen.values()):
                return
            key, value = self.buffer.pop(at)
            self.deliver(key, value)

    def deliver(self, key: str, value: Any) -> None:
        """Apply a message to this node: update the store, append to the log."""
        self.store[key] = value
        self.log.append((key, value))

    # Network handler
    def on_message(self, msg: Message) -> None:
        if msg["type"] == "replicate":
            self.clock = max(self.clock, msg["ts"], msg["sender_clock"]) + 1
            self.last_seen[self.node_id] = self.clock
            sender = msg["from"]
            self.last_seen[sender] = max(self.last_seen[sender], msg["sender_clock"])
            self.last_seen[msg["origin"]] = max(self.last_seen[msg["origin"]], msg["ts"])
            self.receive_and_replicate(msg["key"], msg["value"], msg["ts"], msg["origin"])
            self.deliver_ready()
        else:
            raise ValueError(f"Unknown message type in {msg!r}")

    # What the trace and diagrams show as this node's state
    def brief_state(self) -> dict[str, Any]:
        return dict(self.store)
