from collections import Counter

import pytest

from dslabs import Cluster, delay, drop, duplicate, partition
from dslabs.job_client import JobClient
from dslabs.nodes import NodeJobs


ACTION = "def run(item):\n    return item * item"


def setup(nodes=3, seed=0):
    cluster = Cluster(NodeJobs, nodes, seed=seed)
    return cluster, JobClient(cluster)


def submit(client, node="n2", job_id="squares", data=None, action=ACTION):
    return client.request(node, {"type": "submit_job", "job_id": job_id,
                                 "job_action": action, "job_data": [2, 3, 4] if data is None else data})


def assert_finished(cluster, expected):
    assert cluster.run_until_idle(cluster.scheduler.now_ms() + 60000)
    counts = Counter()
    for node in cluster.nodes.values():
        counts.update(node.executions)
        assert [task["result"] for task in node.jobs["squares"]["tasks"]] == expected
        assert len(node.completions) == len(expected)
        assert len({(j, i) for j, i, _ in node.completions}) == len(expected)
    assert counts == Counter({("squares", i): 1 for i in range(len(expected))})
    logs = [node.log for node in cluster.nodes.values()]
    assert all(log == logs[0] for log in logs)


def test_cas_has_one_winner_and_retries_do_not_apply_twice():
    cluster, client = setup()
    cluster.add_rule(duplicate(1))
    ids = [client.request(n, {"type": "cas", "key": "lock", "expected": None, "value": n})
           for n in cluster.node_ids]
    replies = [client.wait(r) for r in ids]
    assert sum(r["swapped"] for r in replies) == 1
    assert cluster.run_until_idle()
    assert len({n.registers["user:lock"] for n in cluster.nodes.values()}) == 1


@pytest.mark.parametrize("seed", [0, 7, 42])
def test_jobs_with_loss_duplicates_and_delay(seed):
    cluster, client = setup(seed=seed)
    cluster.add_rule(drop(0.5))
    cluster.add_rule(duplicate(0.5))
    cluster.add_rule(delay(0, 200))
    request = submit(client)
    assert client.wait(request)["job_submitted"] is True
    assert_finished(cluster, [4, 9, 16])
    result = client.request("n3", {"type": "get_job_results", "job_id": "squares"})
    assert client.wait(result)["job_results"] == [4, 9, 16]


def test_reads_wait_for_ordered_state_after_submission():
    cluster, client = setup()
    cut = partition({"n3"})
    cluster.add_rule(cut)
    assert client.wait(submit(client))["job_submitted"]
    assert "squares" not in cluster["n3"].jobs
    request = client.request("n3", {"type": "query_job_status", "job_id": "squares"})
    cluster.run_until(cluster.scheduler.now_ms() + 1500)
    assert request not in client.responses
    cluster.remove_rule(cut)
    assert client.wait(request)["job_status"] in {"in_progress", "complete"}
    assert_finished(cluster, [4, 9, 16])


def test_submission_waits_when_sequencer_is_partitioned():
    cluster, client = setup()
    cut = partition({"n1"})
    cluster.add_rule(cut)
    request = submit(client)
    cluster.run_until(1500)
    assert request not in client.responses
    cluster.remove_rule(cut)
    assert client.wait(request)["job_submitted"]
    assert_finished(cluster, [4, 9, 16])


def test_repeated_job_id_and_conflicting_submission():
    cluster, client = setup()
    ids = [submit(client, node=n) for n in cluster.node_ids]
    assert all(client.wait(r)["job_submitted"] for r in ids)
    conflict = submit(client, data=[99])
    assert client.wait(conflict)["job_submitted"] is False
    assert_finished(cluster, [4, 9, 16])


def test_unknown_empty_and_failed_jobs():
    cluster, client = setup()
    query = client.request("n3", {"type": "query_job_status", "job_id": "missing"})
    assert client.wait(query)["job_status"] == "not_found"
    assert client.wait(submit(client, data=[]))["job_submitted"]
    status = client.request("n1", {"type": "query_job_status", "job_id": "squares"})
    assert client.wait(status)["job_status"] == "complete"
    bad = submit(client, job_id="error", action="def run(item):\n    raise ValueError('bad input')")
    assert client.wait(bad)["job_submitted"]
    assert cluster.run_until_idle()
    results = client.request("n2", {"type": "get_job_results", "job_id": "error"})
    assert all(r["error"] == "ValueError" for r in client.wait(results)["job_results"])


def test_one_node_cluster_goes_idle():
    cluster, client = setup(nodes=1)
    assert client.wait(submit(client, node="n1"))["job_submitted"]
    assert_finished(cluster, [4, 9, 16])


def test_lost_client_responses_and_identical_inputs():
    cluster, client = setup()
    def lose_responses(frm, to, deliveries, rng):
        return [] if to == "client" else deliveries
    cluster.add_rule(lose_responses)
    request = submit(client, data=[2, 2, 2])
    cluster.run_until(2000)
    assert request not in client.responses
    cluster.remove_rule(lose_responses)
    assert client.wait(request)["job_submitted"]
    assert_finished(cluster, [4, 4, 4])


def test_original_message_schema_without_request_id():
    cluster, client = setup()
    received = []
    cluster.network.register("raw_client", received.append)
    cluster.network.endpoint("raw_client").send("n2", {
        "type": "submit_job", "job_id": "squares", "job_action": ACTION, "job_data": [2, 3, 4]})
    assert_finished(cluster, [4, 9, 16])
    assert received[0]["job_submitted"]
