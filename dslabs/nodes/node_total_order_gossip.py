import random
from dataclasses import dataclass, field
from typing import Any

from dslabs.nodes.node_total_order_eager_broadcast import NodeTotalOrderEagerBroadcast


@dataclass
class NodeTotalOrderGossip(NodeTotalOrderEagerBroadcast): # I inherited from NodeTotalOrderEagerBroadcast to save time
    """Total order broadcast where replication is gossiped"""

    num_gossip_peers: int = 3
    num_gossip_rounds: int = 3
    gossip_interval_ms: int = 100
    rng: random.Random = field(init=False, repr=False, default=None)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.rng = random.Random()

    def receive_and_replicate(self, key: str, value: Any, ts: int, origin: str) -> None:
        """Buffer a write the first time we see it, then start gossiping it."""
        if (ts, origin) in self.seen:
            return
        self.seen.add((ts, origin))
        self.buffer[(ts, origin)] = (key, value)
        self.gossip(key, value, ts, origin, 0)

    def gossip(self, key: str, value: Any, ts: int, origin: str, round_no: int) -> None:
        others = [p for p in self.peers if p != self.node_id]
        for peer in self.rng.sample(others, min(self.num_gossip_peers, len(others))):
            self.transport.send(peer, {"type": "replicate", "key": key, "value": value,
                                       "ts": ts, "origin": origin, "sender_clock": self.clock})
        if round_no + 1 < self.num_gossip_rounds:
            # Rounds are spread over time on purpose: a later round resends what
            # the network may have dropped, and carries a higher clock with it.
            def next_round() -> None:
                self.gossip(key, value, ts, origin, round_no + 1)

            next_round.description = f"gossip round {round_no + 1} of ({ts}, {origin}) at {self.node_id}"
            self.scheduler.call_later(self.gossip_interval_ms, next_round)
