"""A simulated client that retries requests with the same request id."""

import copy


class JobClient:
    def __init__(self, cluster, client_id="client", retry_ms=500):
        self.cluster = cluster
        self.client_id = client_id
        self.retry_ms = retry_ms
        self.pending = {}
        self.responses = {}
        self.counter = 0
        cluster.network.register(client_id, self.on_message)
        self.transport = cluster.network.endpoint(client_id)

    def request(self, node_id, message):
        self.counter += 1
        request_id = f"{self.client_id}:{self.counter}"
        message = dict(copy.deepcopy(message), request_id=request_id)
        self.pending[request_id] = (node_id, message)
        self.send(request_id)
        return request_id

    def send(self, request_id):
        if request_id in self.pending:
            node, message = self.pending[request_id]
            self.transport.send(node, message)
            self.cluster.scheduler.call_later(self.retry_ms, lambda: self.send(request_id))

    def on_message(self, message):
        request_id = message["request_id"]
        self.responses.setdefault(request_id, message)
        self.pending.pop(request_id, None)

    def wait(self, request_id, timeout_ms=60000):
        deadline = self.cluster.scheduler.now_ms() + timeout_ms
        while request_id not in self.responses:
            event = self.cluster.peek()
            if event is None or event.due_ms > deadline:
                raise TimeoutError(request_id)
            self.cluster.step()
        return self.responses[request_id]
