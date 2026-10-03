"""Ordered CAS and a job queue on the existing fixed-leader TOB simulator."""

import copy
import json
from dataclasses import dataclass, field
from typing import Any

from .node_total_order import NodeTotalOrder


@dataclass
class NodeJobs(NodeTotalOrder):
    """CAS and jobs over TOB. Job code defines run(item).

    Handles network failures with nodes alive. The log is in memory;
    permanent crashes and recovery of Python side effects are not handled.
    """

    jobs: dict[str, dict] = field(default_factory=dict)
    registers: dict[str, Any] = field(default_factory=dict)
    replies: dict[str, dict] = field(default_factory=dict)
    completions: list[tuple[str, int, Any]] = field(default_factory=list)
    executions: dict[tuple[str, int], int] = field(default_factory=dict)
    api_counter: int = 0

    def cas(self, key: str, expected: Any, value: Any) -> bool:
        """State-machine step; call only while delivering an ordered command."""
        if self.registers.get(key) != expected:
            return False
        self.registers[key] = copy.deepcopy(value)
        return True

    def propose(self, command: dict) -> None:
        # Defer to avoid recursively delivering inside the parent's delivery loop.
        self.scheduler.call_later(0, lambda: self.client_put("__jobs__", command))

    def on_message(self, msg: dict) -> None:
        if msg["type"] in {"submit_job", "query_job_status", "get_job_results", "cas"}:
            command = {k: copy.deepcopy(v) for k, v in msg.items() if k != "from"}
            self.api_counter += 1
            command.setdefault("request_id", f"api:{self.node_id}:{self.api_counter}")
            command["reply_to"] = msg["from"]
            command["ingress"] = self.node_id
            self.propose(command)
        else:
            super().on_message(msg)

    def assign(self, key, value, origin, send_seq):
        super().assign(key, value, origin, send_seq)
        # A one-node cluster has nobody to acknowledge its writes.
        self.unacked = {seq: waiting for seq, waiting in self.unacked.items() if waiting}

    def deliver(self, key: str, value: Any) -> None:
        if key != "__jobs__":
            super().deliver(key, value)
            return
        command = copy.deepcopy(value)
        # Retries can appear in the transport log, but affect the application once.
        self.log.append((key, command))
        kind = command["type"]
        request_id = command.get("request_id")
        if request_id is not None:
            if request_id not in self.replies:
                self.replies[request_id] = self.apply_api(command)
            if command["ingress"] == self.node_id:
                reply = dict(self.replies[request_id], request_id=request_id)
                self.transport.send(command["reply_to"], reply)
        elif kind == "claim":
            job_id, index = command["job_id"], command["index"]
            task = self.jobs[job_id]["tasks"][index]
            won = self.cas(task["owner_key"], None, command["worker"])
            if won and command["worker"] == self.node_id:
                self.scheduler.call_later(0, lambda: self.execute(job_id, index))
        elif kind == "finish":
            job_id, index = command["job_id"], command["index"]
            task = self.jobs[job_id]["tasks"][index]
            if not task["done"] and self.registers.get(task["owner_key"]) == command["worker"]:
                task["done"] = True
                task["result"] = command["result"]
                self.completions.append((job_id, index, command["result"]))

    def apply_api(self, command: dict) -> dict:
        kind = command["type"]
        if kind == "cas":
            # Keep user registers separate from task ownership registers.
            key = "user:" + command["key"]
            ok = self.cas(key, command["expected"], command["value"])
            return {"type": "cas_response", "swapped": ok,
                    "value": copy.deepcopy(self.registers.get(key))}
        job_id = command["job_id"]
        job = self.jobs.get(job_id)
        response = {"type": kind + "_response", "job_id": job_id}
        if kind == "submit_job":
            action, data = command.get("job_action"), command.get("job_data")
            try:
                if not isinstance(action, str) or not isinstance(data, list):
                    raise ValueError("Expected Python source and a list of inputs")
                compile(action, "<job>", "exec")
                json.dumps(data, allow_nan=False)
                if job is not None and (job["action"] != action or job["data"] != data):
                    raise ValueError("job_id already used for a different job")
                if job is None:
                    self.jobs[job_id] = {"action": action, "data": data, "tasks": [
                        {"owner_key": "task:" + json.dumps([job_id, i]), "done": False}
                        for i in range(len(data))]}
                    for index in range(len(data)):
                        self.propose({"type": "claim", "job_id": job_id,
                                      "index": index, "worker": self.node_id})
                response["job_submitted"] = True
            except (ValueError, TypeError, SyntaxError) as error:
                response.update(job_submitted=False, error=str(error))
        elif kind == "query_job_status":
            response["job_status"] = ("not_found" if job is None else
                                      "complete" if all(t["done"] for t in job["tasks"]) else
                                      "in_progress")
        elif kind == "get_job_results":
            complete = job is not None and all(t["done"] for t in job["tasks"])
            response["job_results"] = ([copy.deepcopy(t["result"]) for t in job["tasks"]]
                                       if complete else None)
        return response

    def execute(self, job_id: str, index: int) -> None:
        job = self.jobs[job_id]
        identity = (job_id, index)
        self.executions[identity] = self.executions.get(identity, 0) + 1
        try:
            namespace = {}
            exec(job["action"], namespace)
            result = namespace["run"](copy.deepcopy(job["data"][index]))
            # Normalize results so the origin and remote replicas store the same types.
            result = json.loads(json.dumps(result, allow_nan=False))
        except Exception as error:
            result = {"error": type(error).__name__, "message": str(error)}
        self.propose({"type": "finish", "job_id": job_id, "index": index,
                      "worker": self.node_id, "result": result})
