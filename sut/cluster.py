"""System under test: a coordinator that dispatches inspection jobs to worker nodes.

Modelled on a central server driving many tool nodes:
  * nodes connect over TCP, register, and heartbeat every second;
  * the coordinator assigns jobs (one per idle node) with a lease, re-dispatches a job when its lease
    expires or its node is declared dead, and records each job's result exactly once;
  * an HTTP endpoint (GET /status, POST /jobs?count=N) lets tests submit work and check invariants.

Three classic distributed-system defects can be switched back on, so the test harness can be
validated against bugs it must catch (env vars, all off by default):
  BUG_DOUBLE_COUNT=1     results are not de-duplicated: when a slow node's job was re-dispatched, its late
                         result is counted a second time
  BUG_TIGHT_TIMEOUT=1    failure timeout of 1.5 s against a 1 s heartbeat: fine on a quiet LAN, but healthy
                         nodes are declared dead (and their jobs re-run) under network delay and jitter
  BUG_CONN_ONLY_DETECT=1 failures are detected only when a TCP connection closes (no heartbeat timeout, no
                         lease): a frozen node or a silent partition keeps its connection open, so its job hangs

Stdlib only. Run: python cluster.py coordinator | python cluster.py node
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import random
import socket
import sys
import time
from dataclasses import dataclass, field

HEARTBEAT_S = 1.0
NODE_TIMEOUT_S = 1.5 if os.environ.get("BUG_TIGHT_TIMEOUT") == "1" else 4.0
LEASE_S = float(os.environ.get("LEASE_S", "6"))
DOUBLE_COUNT = os.environ.get("BUG_DOUBLE_COUNT") == "1"
CONN_ONLY = os.environ.get("BUG_CONN_ONLY_DETECT") == "1"


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


# --------------------------------------------------------------------------------------- coordinator
@dataclass
class Job:
    id: int
    work_ms: int
    state: str = "queued"  # queued / running / done
    node: str | None = None
    lease_until: float = 0.0
    attempts: int = 0
    result: str | None = None


@dataclass
class Node:
    id: str
    writer: asyncio.StreamWriter
    last_seen: float
    alive: bool = True
    job: int | None = None


@dataclass
class Coordinator:
    jobs: dict[int, Job] = field(default_factory=dict)
    nodes: dict[str, Node] = field(default_factory=dict)
    completions: int = 0         # result messages accepted as completing a job
    duplicate_results: int = 0   # results for jobs that were already done (ignored unless BUG_DOUBLE_COUNT)
    node_deaths: int = 0
    redispatches: int = 0
    next_id: int = 1

    async def handle_node(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        node: Node | None = None
        try:
            while line := await reader.readline():
                msg = json.loads(line)
                now = time.monotonic()
                if msg["t"] == "register":
                    old = self.nodes.get(msg["node"])
                    if old is not None and old.job is not None:
                        self.requeue(old.job, "node re-registered")
                    node = Node(msg["node"], writer, now)
                    self.nodes[node.id] = node
                    log("registered", node.id)
                elif node is None:
                    continue
                elif msg["t"] == "hb":
                    node.last_seen = now
                    if not node.alive:
                        node.alive = True
                        log("node back", node.id)
                elif msg["t"] == "result":
                    node.last_seen = now
                    self.on_result(node, msg["job"], msg["value"])
                self.dispatch()
        except (ConnectionError, json.JSONDecodeError):
            pass
        finally:
            if node is not None and self.nodes.get(node.id) is node:
                self.mark_dead(node, "connection closed")

    def on_result(self, node: Node, job_id: int, value: str):
        job = self.jobs.get(job_id)
        if node.job == job_id:
            node.job = None
        if job is None:
            return
        if job.state == "done":
            self.duplicate_results += 1
            if not DOUBLE_COUNT:
                return  # idempotent: a second result for a finished job changes nothing
        job.state, job.result, job.node = "done", value, node.id
        self.completions += 1

    def requeue(self, job_id: int, why: str):
        job = self.jobs[job_id]
        if job.state == "running":
            job.state, job.node = "queued", None
            self.redispatches += 1
            log("requeue job", job_id, why)

    def mark_dead(self, node: Node, why: str):
        if node.alive:
            node.alive = False
            self.node_deaths += 1
            log("node dead", node.id, why)
        if node.job is not None:
            self.requeue(node.job, f"node {node.id} dead")
            node.job = None

    def dispatch(self):
        idle = [n for n in self.nodes.values() if n.alive and n.job is None]
        for job in self.jobs.values():
            if not idle:
                return
            if job.state != "queued":
                continue
            n = idle.pop(0)
            job.state, job.node, job.attempts = "running", n.id, job.attempts + 1
            job.lease_until = time.monotonic() + LEASE_S
            n.job = job.id
            try:
                n.writer.write((json.dumps({"t": "assign", "job": job.id, "work_ms": job.work_ms}) + "\n").encode())
            except ConnectionError:
                self.mark_dead(n, "write failed")

    async def supervise(self):
        while True:
            await asyncio.sleep(0.2)
            now = time.monotonic()
            if CONN_ONLY:
                self.dispatch()
                continue  # the defect: rely on connection close alone
            for n in list(self.nodes.values()):
                if n.alive and now - n.last_seen > NODE_TIMEOUT_S:
                    self.mark_dead(n, f"no heartbeat for {now - n.last_seen:.1f}s")
            for job in self.jobs.values():
                if job.state == "running" and now > job.lease_until:
                    holder = self.nodes.get(job.node or "")
                    if holder is not None and holder.job == job.id:
                        holder.job = None
                    self.requeue(job.id, "lease expired")
            self.dispatch()

    def status(self) -> dict:
        done = sum(j.state == "done" for j in self.jobs.values())
        return {"jobs": len(self.jobs), "done": done, "completions": self.completions,
                "duplicate_results": self.duplicate_results, "redispatches": self.redispatches,
                "node_deaths": self.node_deaths,
                "nodes_alive": sum(n.alive for n in self.nodes.values()), "nodes_known": len(self.nodes),
                "max_attempts": max((j.attempts for j in self.jobs.values()), default=0),
                "bugs": {"double_count": DOUBLE_COUNT, "tight_timeout": NODE_TIMEOUT_S < 2, "conn_only_detect": CONN_ONLY}}

    async def handle_http(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            request = (await reader.readline()).decode()
            while (await reader.readline()) not in (b"\r\n", b""):
                pass
            method, path, _ = request.split(" ", 2)
            if method == "POST" and path.startswith("/jobs"):
                count = int(path.split("count=")[1]) if "count=" in path else 1
                for _ in range(count):
                    jid = self.next_id
                    self.next_id += 1
                    self.jobs[jid] = Job(jid, work_ms=random.randint(200, 600))
                self.dispatch()
                body, code = {"submitted": count}, "200 OK"
            elif method == "GET" and path == "/status":
                body, code = self.status(), "200 OK"
            else:
                body, code = {"error": "not found"}, "404 Not Found"
            data = json.dumps(body).encode()
            writer.write(f"HTTP/1.1 {code}\r\nContent-Type: application/json\r\nContent-Length: {len(data)}\r\n"
                         f"Connection: close\r\n\r\n".encode() + data)
            await writer.drain()
        except (ValueError, ConnectionError):
            pass
        finally:
            writer.close()


async def coordinator():
    c = Coordinator()
    await asyncio.start_server(c.handle_node, "0.0.0.0", 9000)
    await asyncio.start_server(c.handle_http, "0.0.0.0", 8080)
    log("coordinator up; node timeout", NODE_TIMEOUT_S, "lease", LEASE_S, "bugs", c.status()["bugs"])
    await c.supervise()


# --------------------------------------------------------------------------------------- node
async def node():
    node_id = os.environ.get("NODE_ID") or socket.gethostname()
    host = os.environ.get("COORDINATOR", "coordinator")
    while True:
        try:
            reader, writer = await asyncio.open_connection(host, 9000)
            sock = writer.get_extra_info("socket")
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            writer.write((json.dumps({"t": "register", "node": node_id}) + "\n").encode())
            log("connected as", node_id)

            async def heartbeat(w: asyncio.StreamWriter):
                while True:
                    w.write(b'{"t": "hb"}\n')
                    await w.drain()
                    await asyncio.sleep(HEARTBEAT_S)

            hb = asyncio.create_task(heartbeat(writer))
            try:
                while line := await reader.readline():
                    msg = json.loads(line)
                    if msg["t"] == "assign":
                        await asyncio.sleep(msg["work_ms"] / 1000)  # the "inspection"
                        value = hashlib.sha256(f"{msg['job']}".encode()).hexdigest()[:16]
                        writer.write((json.dumps({"t": "result", "job": msg["job"], "value": value}) + "\n").encode())
                        await writer.drain()
            finally:
                hb.cancel()
        except (ConnectionError, OSError):
            pass
        await asyncio.sleep(0.5)


if __name__ == "__main__":
    asyncio.run(coordinator() if sys.argv[1] == "coordinator" else node())
