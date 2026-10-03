from dataclasses import dataclass, field
from typing import Any

from dslabs.protocols import Message, Scheduler, Transport


@dataclass
class NodeTotalOrder:
    """FIFO-total-order broadcast using a sequencer.    """

    node_id: str
    peers: list[str]
    transport: Transport
    scheduler: Scheduler
    store: dict[str, Any] = field(default_factory=dict)
    log: list[tuple[str, Any]] = field(default_factory=list)
    # Requests already forwarded, as (origin, send_seq), so one is not flooded twice
    seen_requests: set[tuple[str, int]] = field(default_factory=set)
    # Numbered writes this node has received, by sequence number
    history: dict[int, tuple[str, Any, str, int]] = field(default_factory=dict)
    # The next sequence number this node will deliver
    next_delivery: int = 0
    # How many writes this node has broadcast, and the ones not delivered yet
    send_seq: int = 0
    pending: dict[int, tuple[str, Any]] = field(default_factory=dict)
    # Sequencer only: the next number to hand out, the next write it expects from
    # each sender, requests that arrived early, and who still owes an ack
    next_seq: int = 0
    expected: dict[str, int] = field(default_factory=dict)
    early: dict[tuple[str, int], tuple[str, Any]] = field(default_factory=dict)
    unacked: dict[int, set[str]] = field(default_factory=dict)
    retry_ms: int = 300
    retry_armed: bool = False

    @property
    def sequencer_id(self) -> str:
        return self.peers[0]

    # Client-facing API
    def client_put(self, key: str, value: Any) -> None:
        send_seq = self.send_seq
        self.send_seq += 1
        self.pending[send_seq] = (key, value)
        self.send_request(key, value, send_seq)
        self.arm_retry()

    def client_get(self, key: str) -> Any:
        return self.store.get(key)

    # Node internals
    def send_request(self, key: str, value: Any, send_seq: int, direct: bool = False) -> None:
        if self.node_id == self.sequencer_id:
            self.receive_request(key, value, self.node_id, send_seq)
            return
        msg = {"type": "request", "key": key, "value": value,
               "origin": self.node_id, "send_seq": send_seq}
        if direct:
            self.transport.send(self.sequencer_id, msg)
        else:
            self.broadcast(msg)

    def receive_request(self, key: str, value: Any, origin: str, send_seq: int) -> None:
        """Sequencer only: number a sender's writes in the order that sender made them."""
        if send_seq < self.expected.get(origin, 0):
            return
        self.early[(origin, send_seq)] = (key, value)
        while (origin, self.expected.get(origin, 0)) in self.early:
            at = self.expected.get(origin, 0)
            key, value = self.early.pop((origin, at))
            self.expected[origin] = at + 1
            self.assign(key, value, origin, at)
        self.arm_retry()

    def assign(self, key: str, value: Any, origin: str, send_seq: int) -> None:
        """Sequencer only: give a write the next number and send it to everyone."""
        seq = self.next_seq
        self.next_seq += 1
        self.unacked[seq] = {p for p in self.peers if p != self.node_id}
        self.receive_ordered(key, value, seq, origin, send_seq)
        for peer in self.peers:
            if peer != self.node_id:
                self.send_ordered(peer, seq)

    def send_ordered(self, to: str, seq: int) -> None:
        key, value, origin, send_seq = self.history[seq]
        self.transport.send(to, {"type": "ordered", "key": key, "value": value,
                                 "seq": seq, "origin": origin, "send_seq": send_seq})

    def receive_ordered(self, key: str, value: Any, seq: int, origin: str, send_seq: int) -> None:
        if seq in self.history:
            return
        self.history[seq] = (key, value, origin, send_seq)
        while self.next_delivery in self.history:
            k, v, who, at = self.history[self.next_delivery]
            self.deliver(k, v)
            if who == self.node_id:
                self.pending.pop(at, None)
            self.next_delivery += 1

    def broadcast(self, msg: Message) -> None:
        for peer in self.peers:
            if peer != self.node_id:
                self.transport.send(peer, msg)

    def deliver(self, key: str, value: Any) -> None:
        """Apply a message to this node: update the store, append to the log."""
        self.store[key] = value
        self.log.append((key, value))

    # Retries: resend anything still outstanding until it is answered
    def arm_retry(self) -> None:
        if not self.retry_armed and (self.pending or self.unacked):
            self.retry_armed = True
            self.scheduler.call_later(self.retry_ms, self.retry)

    def retry(self) -> None:
        self.retry_armed = False
        for send_seq, (key, value) in list(self.pending.items()):
            # Straight at the sequencer this time, the first broadcast did not do it
            self.send_request(key, value, send_seq, direct=True)
        for seq, waiting in list(self.unacked.items()):
            for peer in self.peers:
                if peer in waiting:
                    self.send_ordered(peer, seq)
        self.arm_retry()

    # Network handler
    def on_message(self, msg: Message) -> None:
        if msg["type"] == "request":
            if self.node_id == self.sequencer_id:
                self.receive_request(msg["key"], msg["value"], msg["origin"], msg["send_seq"])
            elif (msg["origin"], msg["send_seq"]) not in self.seen_requests:
                self.seen_requests.add((msg["origin"], msg["send_seq"]))
                self.broadcast(msg)
        elif msg["type"] == "ordered":
            # Acknowledge every copy, including one we already have: if an
            # acknowledgement is lost the sequencer will send the write again.
            self.transport.send(msg["from"], {"type": "ack", "seq": msg["seq"]})
            self.receive_ordered(msg["key"], msg["value"], msg["seq"], msg["origin"], msg["send_seq"])
        elif msg["type"] == "ack":
            waiting = self.unacked.get(msg["seq"])
            if waiting is not None:
                waiting.discard(msg["from"])
                if not waiting:
                    del self.unacked[msg["seq"]]
        else:
            raise ValueError(f"Unknown message type in {msg!r}")

    # What the trace and diagrams show as this node's state
    def brief_state(self) -> dict[str, Any]:
        return dict(self.store)
