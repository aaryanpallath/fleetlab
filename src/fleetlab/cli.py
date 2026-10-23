"""fleetlab CLI.

  fleetlab up TOPOLOGY [--nodes N]     start an environment and print how to reach it (Ctrl-C tears it down)
  fleetlab sweep                       remove anything left behind by crashed runs
  fleetlab bench TOPOLOGY [--sizes 5,10,20,40] [--repeats 3] [--jobs 200] [--out results/bench.json]
"""
from __future__ import annotations

import argparse
import json
import platform
import statistics
import subprocess
import sys
import time
from pathlib import Path

from .cluster import Cluster, sweep
from .topology import Topology


def _status(c: Cluster) -> dict:
    return c.get("coordinator", 8080, "/status")


def bench_once(topology: Path, nodes: int, jobs: int) -> dict:
    t0 = time.monotonic()
    c = Cluster(Topology.load(topology, replicas={"node": nodes}))
    c.up()
    try:
        healthy = c.wait_until(lambda: _status(c)["nodes_alive"] == nodes, timeout=180, what=f"{nodes} nodes")
        stats = c.stats()
        t_jobs = time.monotonic()
        c.http("coordinator", 8080, f"/jobs?count={jobs}", method="POST")
        c.wait_until(lambda: _status(c)["done"] == jobs, timeout=300, interval=0.1, what=f"{jobs} jobs")
        jobs_s = time.monotonic() - t_jobs
        st = _status(c)
    finally:
        c.down()
    return {
        "nodes": nodes, "containers": nodes + 1, "up_s": round(c.timing.up_s, 2),
        "ready_s": round(healthy, 2), "teardown_s": round(c.timing.down_s, 2),
        "total_cycle_s": round(time.monotonic() - t0, 2),
        "mem_total_mib": round(sum(s["mem_mib"] for s in stats.values()), 1),
        "mem_per_node_mib": round(statistics.mean(v["mem_mib"] for k, v in stats.items() if k != "coordinator"), 1),
        "jobs": jobs, "jobs_s": round(jobs_s, 2), "exactly_once": st["completions"] == jobs == st["done"],
    }


def bench(args) -> int:
    sizes = [int(x) for x in args.sizes.split(",")]
    runs = []
    for n in sizes:
        for r in range(args.repeats):
            res = bench_once(Path(args.topology), n, args.jobs)
            res["repeat"] = r + 1
            runs.append(res)
            print(json.dumps(res), flush=True)
    summary = []
    for n in sizes:
        rs = [r for r in runs if r["nodes"] == n]
        med = {k: statistics.median(r[k] for r in rs) for k in
               ("up_s", "ready_s", "teardown_s", "total_cycle_s", "mem_total_mib", "mem_per_node_mib", "jobs_s")}
        summary.append({"nodes": n, "containers": n + 1, "repeats": len(rs),
                        "all_exactly_once": all(r["exactly_once"] for r in rs), **med})
    docker_v = subprocess.run(["docker", "version", "--format", "{{.Server.Version}}"], capture_output=True,
                              text=True, check=False).stdout.strip()
    info = subprocess.run(["docker", "info", "--format", "{{.NCPU}} {{.MemTotal}}"], capture_output=True,
                          text=True, check=False).stdout.split()
    out = {"generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
           "host": f"{platform.system()} {platform.machine()}", "docker": docker_v,
           "docker_vm_cpus": int(info[0]) if info else None,
           "docker_vm_mem_gib": round(int(info[1]) / 2**30, 1) if len(info) > 1 else None,
           "note": "medians; image already built (warm), so startup excludes image pulls/builds",
           "summary": summary, "runs": runs}
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(out, indent=2) + "\n")
    print("wrote", args.out)
    return 0


def up(args) -> int:
    replicas = {"node": args.nodes} if args.nodes else None
    with Cluster(Topology.load(args.topology, replicas=replicas)) as c:
        print(f"project {c.project}; compose file {c.compose_file}")
        for svc in c.topology.services.values():
            for p in svc.publish:
                print(f"{svc.name}:{p} -> http://127.0.0.1:{c.port(svc.name, p)}")
        print("Ctrl-C to tear down")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="fleetlab")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("up")
    p.add_argument("topology")
    p.add_argument("--nodes", type=int)
    sub.add_parser("sweep")
    b = sub.add_parser("bench")
    b.add_argument("topology")
    b.add_argument("--sizes", default="5,10,20,40")
    b.add_argument("--repeats", type=int, default=3)
    b.add_argument("--jobs", type=int, default=200)
    b.add_argument("--out", default="results/bench.json")
    args = ap.parse_args(argv)
    if args.cmd == "sweep":
        print(f"removed {sweep()} containers")
        return 0
    return up(args) if args.cmd == "up" else bench(args)


if __name__ == "__main__":
    sys.exit(main())
