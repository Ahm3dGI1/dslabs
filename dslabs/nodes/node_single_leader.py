from dataclasses import dataclass, field
from typing import Any

from dslabs.protocols import Message, Scheduler, Transport


@dataclass
class NodeSingleLeader:
    """A node in a single-leader cluster.

    The leader stamps each write with a per-key ``order`` number and broadcasts it.
    A node applies a write only if its ``order`` is newer than the last one it
    applied for that key, so a replication message that arrives late is ignored
    instead of overwriting a newer value.
    """
    node_id: str                  # this node's name
    peers: list[str]              # every node in teh cluster including this one
    transport: Transport
    scheduler: Scheduler
    leader_id: str | None = None  # refrence to the leader id, forward all client requests to the leader
    # Key-value store
    store: dict[str, Any] = field(default_factory=dict)
    # Append-only log of everything delivered, in delivery order
    log: list[tuple[str, Any]] = field(default_factory=list)
    # Highest order applied per key; on the leader, also the last order handed out
    applied: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.leader_id is None:
            self.leader_id = self.peers[0]

    def is_leader(self) -> bool:
        return self.node_id == self.leader_id

    def client_put(self, key: str, value: Any) -> None:
        if self.is_leader():
            self.receive_and_replicate(key, value, self.applied.get(key, -1) + 1)
        else:
            # follower does not accept writes but forwards them to the leader.
            self.transport.send(self.leader_id, {"type": "forward", "key": key, "value": value})

    def client_get(self, key: str) -> Any:
        return self.store.get(key)

    def receive_and_replicate(self, key: str, value: Any, order: int) -> None:
        self.receive(key, value, order)
        for peer in self.peers:
            if peer != self.node_id:
                self.transport.send(peer, {"type": "replicate", "key": key, "value": value, "order": order})

    def receive(self, key: str, value: Any, order: int) -> None:
        # Received messages are delivered immediately
        self.deliver(key, value, order)

    def deliver(self, key: str, value: Any, order: int) -> None:
        """Apply a message to this node: update the store, append to the log."""
        if order > self.applied.get(key, -1):
            self.applied[key] = order
            self.store[key] = value
            self.log.append((key, value))

    # Network handler
    def on_message(self, msg: Message) -> None:
        if msg["type"] == "replicate":
            self.receive(msg["key"], msg["value"], msg["order"])
        elif msg["type"] == "forward":
            # only the leader may act on it.
            if not self.is_leader():
                raise ValueError(f"{self.node_id} is not the leader but was forwarded {msg!r}")
            self.client_put(msg["key"], msg["value"])
        else:
            raise ValueError(f"Unknown message type in {msg!r}")

    # What the trace and diagrams show as this node's state
    def brief_state(self) -> dict[str, Any]:
        return dict(self.store)