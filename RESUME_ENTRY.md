# fleetlab — On-Demand Containerized Test Environments for Distributed Systems
Tech: Python, Docker, Docker Compose, Linux networking (tc netem, iptables), pytest, CI/CD (GitHub Actions)
- Built a Python harness that starts a distributed system (central server + N nodes) as containers on one machine for each test, injects delay, jitter, packet loss, silent partitions and frozen or killed nodes, and always tears the environment down.
- Brought up a 1 + 20 node environment in 1.82 s using 411 MiB, and scaled to 40 nodes with every job completed exactly once.
- Validated the harness by re-creating three classic distributed-system defects (double-counted results, a failure timeout too tight for real networks, connection-only failure detection); each is caught, and the fixed code passes, in CI on every push.
