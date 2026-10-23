# fleetlab — on-demand containerized test environments for distributed systems

System-level tests of distributed software usually need real machines or a long-lived fleet of VMs.
fleetlab instead starts the whole system (a central server and its nodes) as containers on one machine
for each test, injects network delay, jitter, loss, partitions, frozen and killed nodes, checks the
system's invariants, and tears everything down when the test ends, including when it fails.

It ships with a system under test modelled on a central server driving many tool nodes, and a test
suite that **re-creates three classic distributed-system defects and catches each one**.

## Results

### Scaling ([`results/bench.json`](results/bench.json))
Median of 3 runs per size; Apple M4 laptop, Docker 29.7.2 with 10 CPUs / 7.8 GiB; image already built.
"Ready" means every node has registered with the coordinator.

| environment | ready | memory (all containers) | 200 jobs | teardown | exactly-once |
|---|---:|---:|---:|---:|:-:|
| coordinator + 5 nodes | 1.15 s | 116 MiB | 16.08 s | 0.80 s | ✓ |
| coordinator + 10 nodes | 1.55 s | 214 MiB | 8.49 s | 1.06 s | ✓ |
| **coordinator + 20 nodes** | **1.82 s** | **411 MiB** | **4.30 s** | **1.78 s** | ✓ |
| coordinator + 40 nodes | 3.62 s | 804 MiB | 2.37 s | 3.32 s | ✓ |

Each node container uses about 19.6 MiB, and a 21-container environment goes from nothing to ready in
under 2 s. I did not measure a VM-based environment to compare against, so no savings figure is
claimed. These numbers are the container side of that comparison.

### Scenarios ([`results/scenarios.json`](results/scenarios.json))
Each test runs in its own fresh environment (coordinator + 5 nodes).

| scenario | outcome |
|---|---|
| baseline: 60 jobs | 60 completed exactly once, no failures or re-dispatches |
| 300 ± 50 ms delay on every node | 40/40 exactly once, **0** false node failures |
| 15% packet loss on two nodes | 40/40 exactly once |
| silent partition between a node and the coordinator (iptables DROP, no RST) | detected after **4.4 s** (timeout 4 s), its job moved, 30/30 exactly once, node rejoins after healing |
| node killed | its job re-dispatched after **0.19 s** (connection closed), 30/30 exactly once, node rejoins after restart |

### Defects the harness must catch
Each defect is a realistic bug that can be switched back on in the system under test (`BUG_*` env
vars). The harness is validated by running every scenario both ways.

| defect | with the bug | fixed |
|---|---|---|
| **Double counting**: a late result from a node whose job was re-dispatched is counted again | 31 completions for 30 jobs | 30; the late duplicate arrives and is ignored |
| **Timeout too tight for the network**: 1.5 s failure timeout against a 1 s heartbeat | 10 healthy nodes declared dead in 20 s under 500 ± 400 ms jitter | 0 |
| **Connection-only failure detection**: no heartbeat timeout or lease | a frozen node holds its TCP connection open: stuck at 19/20 jobs | all 20 finish; the frozen node is declared dead and its job moves |

## How it works

```
 test ──▶ Cluster(topology).up()  ──▶ docker compose project "fleetlab-<run id>" (private bridge network)
             │                          ├─ coordinator   (status port published on 127.0.0.1:<ephemeral>)
             │                          ├─ node-1 … node-N (stable hostnames, NET_ADMIN for tc/iptables)
             ├─ wait_until(all nodes registered)
             ├─ faults: netem(delay/jitter/loss) · partition(a, b) · freeze/thaw · kill/start · heal
             ├─ invariants over the coordinator's /status
             └─ down()  (context manager, pytest fixture, atexit and SIGTERM handlers)
```

* **Topology files** (`topologies/inspect.yaml`) declare services and replica counts. Each replica
  becomes its own Compose service, so a fault can target `node-7` and its logs are named.
* **Isolation:** every run is a separate Compose project with a unique name and a label, so runs can go
  in parallel on one server. `fleetlab sweep` removes anything a crashed run left behind, and CI
  fails if any environment is left running.
* **Faults** run inside the containers:
  * `tc qdisc … netem` for delay, jitter and loss;
  * `iptables DROP` for partitions, which is silent (no reset), unlike stopping a container;
  * the cgroup freezer (`docker pause`) for a node that is alive at the TCP level but making no progress.
* **System under test** (`sut/cluster.py`, stdlib Python): nodes register and heartbeat; the
  coordinator dispatches jobs with a lease, re-dispatches on node failure or lease expiry, and makes
  results idempotent.

## Setup and usage

Requires Docker and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run pytest -q tests/test_topology.py      # unit tests, no Docker needed
uv run pytest -q -m docker                    # all scenarios (~2 min)
uv run fleetlab bench topologies/inspect.yaml # scaling benchmark -> results/bench.json
uv run fleetlab up topologies/inspect.yaml --nodes 20   # interactive; Ctrl-C tears it down
uv run fleetlab sweep                         # clean up after a crashed run
```

Writing a test:

```python
def test_my_failure_mode(cluster):            # fixture: fresh environment, always torn down
    cluster.submit(30)
    cluster.c.netem("node-2", delay_ms=800, loss_pct=5)
    cluster.c.partition("node-3", "coordinator")
    st = cluster.wait_done(30)
    cluster.assert_exactly_once(st, 30)
```

## Design decisions
* **Compose as the engine, not a custom orchestrator.** It already does networks, health and
  teardown; fleetlab adds per-run isolation, fault injection, readiness, measurement and guaranteed
  cleanup. A topology file can describe a different system without code changes.
* **Partitions with iptables, not by stopping containers.** A stopped container resets its
  connections, so the other side notices immediately. Real network splits are silent, and that is the
  case that exposed the connection-only detection defect.
* **Invariants instead of log scraping.** Tests assert on the system's own state (each job completed
  exactly once, how many failures were declared), so a pass means the property held.
* **Every defect test runs both ways**, so a harness that silently stopped injecting faults would fail
  the "is caught" half.

## Limitations
* The defects were re-created on purpose in a system written for this project; they show the harness
  can expose these classes of bug, not that it found them in someone else's code.
* No VM baseline was measured, so resource and startup savings against VMs are not quantified.
* All containers share one kernel and CPU, so timing-sensitive results depend on the host's load
  (the jitter defect test checks for at least one false failure, not an exact count).
* Faults are injected per node (all of a node's outbound traffic), not per link pair, except partitions.
* The benchmark measures warm starts; image builds and pulls are not included.

## Testing
11 unit tests cover topology parsing, the generated Compose model and Docker memory parsing. 11 system
scenarios run in real containers. CI runs lint, both suites and a 1 + 20 node scaling check on every
push, then fails if any environment was left behind.

## Data & licensing
No external data. MIT licensed.

## AI assistance disclosure
Built with AI assistance (Claude Code). Every number above comes from the committed result files.
While designing the defect tests I dropped a planned "missing lease" defect because the heartbeat
timeout already masked it, so it could not be caught independently.
