import uuid
from dataclasses import dataclass, field
from typing import Any

from dslabs.protocols import Message, Scheduler, Transport


@dataclass
class NodeEagerBroadcast:
    """Multi-leader replication where every node rebroadcasts what it receives.

    A node that sees a message for the first time applies it and forwards it to
    all its peers, so a write reaches a node even when the sender's own copy of
    it was dropped. Each message carries an id, and a node ignores an id it has
    already seen.
    """

    node_id: str
    peers: list[str]
    transport: Transport
    scheduler: Scheduler
    store: dict[str, Any] = field(default_factory=dict)
    log: list[tuple[str, Any]] = field(default_factory=list)
    # Ids of the messages this node has already applied and forwarded
    seen: set[str] = field(default_factory=set)

    # Client-facing API
    def client_put(self, key: str, value: Any) -> None:
        self.receive_and_replicate(key, value, str(uuid.uuid4()))

    def client_get(self, key: str) -> Any:
        return self.store.get(key)

    # Node internals
    def receive_and_replicate(self, key: str, value: Any, msg_id: str) -> None:
        if msg_id in self.seen:
            return
        self.seen.add(msg_id)
        self.receive(key, value)
        for peer in self.peers:
            if peer != self.node_id:
                self.transport.send(peer, {"type": "replicate", "key": key, "value": value, "id": msg_id})

    def receive(self, key: str, value: Any) -> None:
        # Received messages are delivered immediately
        self.deliver(key, value)

    def deliver(self, key: str, value: Any) -> None:
        """Apply a message to this node: update the store, append to the log."""
        self.store[key] = value
        self.log.append((key, value))

    # Network handler
    def on_message(self, msg: Message) -> None:
        if msg["type"] == "replicate":
            self.receive_and_replicate(msg["key"], msg["value"], msg["id"])
        else:
            raise ValueError(f"Unknown message type in {msg!r}")

    # What the trace and diagrams show as this node's state
    def brief_state(self) -> dict[str, Any]:
        return dict(self.store)
