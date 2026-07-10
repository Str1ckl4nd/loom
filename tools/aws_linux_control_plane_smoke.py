#!/usr/bin/env python3
"""Unsupported AWS resource-lifecycle smoke reference.

Automatic cloud resource creation and deletion are outside the supported
project scope. This file is retained for historical validation and community
reference; maintained provisioning support requires a contributor-owned PR.

Creates at most two temporary EC2 Linux instances: one controller and one worker.
It uploads the standard-library Python controller/worker, runs one task through
the private VPC path, prints the summary, and deletes all temporary resources.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any
from urllib.request import urlopen


REFERENCE_NOTICE = (
    "cloud resource lifecycle is outside the supported project scope; "
    "this AWS smoke is retained as an unsupported reference, and maintained "
    "support requires a contributor-owned PR"
)


def run(cmd: list[str], *, timeout: int = 120, check: bool = True, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(cmd, input=input_text, text=True, capture_output=True, timeout=timeout)
    if check and proc.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(cmd)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}")
    return proc


def aws(args: argparse.Namespace, parts: list[str], *, timeout: int = 180, check: bool = True) -> str:
    cmd = ["aws", "--region", args.region, *parts]
    return run(cmd, timeout=timeout, check=check).stdout.strip()


def aws_json(args: argparse.Namespace, parts: list[str], *, timeout: int = 180) -> Any:
    out = aws(args, [*parts, "--output", "json"], timeout=timeout)
    return json.loads(out) if out else {}


def public_ip_cidr() -> str:
    with urlopen("https://checkip.amazonaws.com", timeout=15) as resp:
        ip = resp.read().decode("utf-8").strip()
    return f"{ip}/32"


def restrict_private_key_acl(path: Path) -> None:
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    if os.name != "nt":
        return
    user = os.environ.get("USERNAME") or os.getlogin()
    run(["icacls", str(path), "/inheritance:r"], timeout=30, check=False)
    run(["icacls", str(path), "/grant:r", f"{user}:R"], timeout=30, check=False)
    for principal in ("Everyone", "Users", "Authenticated Users", "BUILTIN\\Users", "*S-1-3-4"):
        run(["icacls", str(path), "/remove:g", principal], timeout=30, check=False)


def ssh(args: argparse.Namespace, key_path: Path, host: str, command: str, *, timeout: int = 180) -> str:
    cmd = [
        "ssh",
        "-i",
        str(key_path),
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "ConnectTimeout=15",
        f"{args.ssh_user}@{host}",
        command,
    ]
    return run(cmd, timeout=timeout).stdout


def scp(args: argparse.Namespace, key_path: Path, host: str, local: Path, remote: str, *, timeout: int = 120) -> None:
    cmd = [
        "scp",
        "-i",
        str(key_path),
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        str(local),
        f"{args.ssh_user}@{host}:{remote}",
    ]
    run(cmd, timeout=timeout)


def wait_ssh(args: argparse.Namespace, key_path: Path, host: str, timeout: int = 240) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            out = ssh(args, key_path, host, "echo ready", timeout=30)
            if "ready" in out:
                return
        except Exception as exc:
            last = exc
            time.sleep(5)
    raise RuntimeError(f"ssh did not become ready for {host}: {last}")


def default_vpc(args: argparse.Namespace) -> str:
    data = aws_json(args, ["ec2", "describe-vpcs", "--filters", "Name=is-default,Values=true"])
    vpcs = data.get("Vpcs") or []
    if not vpcs:
        raise RuntimeError("No default VPC found.")
    return vpcs[0]["VpcId"]


def latest_al2023_ami(args: argparse.Namespace) -> str:
    return aws(
        args,
        [
            "ssm",
            "get-parameter",
            "--name",
            "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64",
            "--query",
            "Parameter.Value",
            "--output",
            "text",
        ],
    )


def create_resources(args: argparse.Namespace, root: Path, resources: dict[str, Any]) -> dict[str, Any]:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%y%m%d-%H%M%S")
    name = f"agentbenchmark-smoke-{stamp}"
    key_name = name
    sg_name = name
    key_path = root / f"{key_name}.pem"
    resources.update({"name": name, "key_name": key_name, "key_path": key_path, "instances": []})
    vpc_id = default_vpc(args)
    key_material = aws(args, ["ec2", "create-key-pair", "--key-name", key_name, "--query", "KeyMaterial", "--output", "text"])
    key_path.write_text(key_material, encoding="utf-8")
    restrict_private_key_acl(key_path)
    sg_id = aws(
        args,
        ["ec2", "create-security-group", "--group-name", sg_name, "--description", "temporary AgentBenchmark smoke", "--vpc-id", vpc_id, "--query", "GroupId", "--output", "text"],
    )
    resources["sg_id"] = sg_id
    cidr = public_ip_cidr()
    aws(args, ["ec2", "authorize-security-group-ingress", "--group-id", sg_id, "--protocol", "tcp", "--port", "22", "--cidr", cidr])
    aws(args, ["ec2", "authorize-security-group-ingress", "--group-id", sg_id, "--protocol", "tcp", "--port", "8765", "--source-group", sg_id])
    ami = latest_al2023_ami(args)
    run_data = aws_json(
        args,
        [
            "ec2",
            "run-instances",
            "--image-id",
            ami,
            "--count",
            "2",
            "--instance-type",
            args.instance_type,
            "--key-name",
            key_name,
            "--security-group-ids",
            sg_id,
            "--instance-initiated-shutdown-behavior",
            "terminate",
            "--tag-specifications",
            f"ResourceType=instance,Tags=[{{Key=Name,Value={name}}},{{Key=Purpose,Value=agentbenchmark-control-plane-smoke}},{{Key=DeleteAfter,Value=immediate}}]",
        ],
        timeout=240,
    )
    instance_ids = [i["InstanceId"] for i in run_data["Instances"]]
    resources["instances"] = [{"instance_id": instance_id} for instance_id in instance_ids]
    aws(args, ["ec2", "wait", "instance-status-ok", "--instance-ids", *instance_ids], timeout=600)
    desc = aws_json(args, ["ec2", "describe-instances", "--instance-ids", *instance_ids])
    instances = []
    for res in desc["Reservations"]:
        for inst in res["Instances"]:
            instances.append(
                {
                    "instance_id": inst["InstanceId"],
                    "public_ip": inst.get("PublicIpAddress"),
                    "private_ip": inst.get("PrivateIpAddress"),
                }
            )
    instances.sort(key=lambda x: x["instance_id"])
    resources["instances"] = instances
    return resources


def cleanup(args: argparse.Namespace, resources: dict[str, Any]) -> None:
    instance_ids = [i["instance_id"] for i in resources.get("instances", [])]
    if instance_ids:
        aws(args, ["ec2", "terminate-instances", "--instance-ids", *instance_ids], check=False, timeout=120)
        aws(args, ["ec2", "wait", "instance-terminated", "--instance-ids", *instance_ids], check=False, timeout=600)
    sg_id = resources.get("sg_id")
    if sg_id:
        aws(args, ["ec2", "delete-security-group", "--group-id", sg_id], check=False, timeout=120)
    key_name = resources.get("key_name")
    if key_name:
        aws(args, ["ec2", "delete-key-pair", "--key-name", key_name], check=False, timeout=120)
    key_path = resources.get("key_path")
    if key_path:
        Path(key_path).unlink(missing_ok=True)


def remote_python_json(args: argparse.Namespace, key: Path, host: str, code: str, timeout: int = 120) -> dict[str, Any]:
    out = ssh(args, key, host, "python3 - <<'PY'\n" + code + "\nPY", timeout=timeout)
    return json.loads(out.strip().splitlines()[-1])


def run_remote_smoke(args: argparse.Namespace, resources: dict[str, Any]) -> dict[str, Any]:
    tool_root = Path(__file__).resolve().parent
    key = Path(resources["key_path"])
    controller = resources["instances"][0]
    worker = resources["instances"][1]
    wait_ssh(args, key, controller["public_ip"])
    wait_ssh(args, key, worker["public_ip"])
    for host in (controller["public_ip"], worker["public_ip"]):
        scp(args, key, host, tool_root / "control_plane_server.py", "/tmp/control_plane_server.py")
        scp(args, key, host, tool_root / "controlled_worker.py", "/tmp/controlled_worker.py")
    ssh(
        args,
        key,
        controller["public_ip"],
        "nohup python3 /tmp/control_plane_server.py server --host 0.0.0.0 --port 8765 --db /tmp/control-plane.sqlite --artifact-root /tmp/control-plane-artifacts > /tmp/control-plane.log 2>&1 &",
    )
    deadline = time.time() + 90
    while True:
        try:
            health = remote_python_json(
                args,
                key,
                controller["public_ip"],
                "import json, urllib.request\nprint(json.dumps(json.load(urllib.request.urlopen('http://127.0.0.1:8765/api/healthz', timeout=5))))",
                timeout=30,
            )
            if health.get("ok"):
                break
        except Exception:
            if time.time() > deadline:
                raise
            time.sleep(3)
    command = "python3 -c \"from pathlib import Path; Path('aws-smoke-output.txt').write_text('ok', encoding='utf-8'); print('aws smoke ok')\""
    ssh(
        args,
        key,
        controller["public_ip"],
        "python3 /tmp/control_plane_server.py dispatch-smoke --controller http://127.0.0.1:8765 --count 1 --prefix aws-smoke --required-capability linux --command " + json.dumps(command),
    )
    worker_out = ssh(
        args,
        key,
        worker["public_ip"],
        f"python3 /tmp/controlled_worker.py --controller http://{controller['private_ip']}:8765 --worker-id aws-worker-1 --capability linux --work-dir /tmp/agentbenchmark-worker --once --max-tasks 1",
        timeout=240,
    )
    summary = remote_python_json(
        args,
        key,
        controller["public_ip"],
        "import json, urllib.request\nbase='http://127.0.0.1:8765'\nout={}\nfor name,path in [('tasks','/api/tasks?limit=20'),('results','/api/data/new-results?cursor=0&limit=20'),('workers','/api/data/active-workers')]:\n    out[name]=json.load(urllib.request.urlopen(base+path, timeout=10))\nprint(json.dumps(out))",
    )
    clean = [t for t in summary.get("tasks", {}).get("tasks", []) if t.get("state") == "clean"]
    if not clean:
        controller_log = ssh(args, key, controller["public_ip"], "cat /tmp/control-plane.log || true", timeout=60)
        raise RuntimeError(f"AWS smoke did not produce a clean task. Worker output={worker_out}\nController log={controller_log}\nSummary={summary}")
    return {"ok": True, "controller": controller, "worker": worker, "worker_output": worker_out, "summary": summary}


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Unsupported AWS resource-lifecycle smoke reference.",
        epilog=REFERENCE_NOTICE,
    )
    parser.add_argument("--region", default="ap-southeast-2")
    parser.add_argument("--instance-type", default="t3.micro")
    parser.add_argument("--ssh-user", default="ec2-user")
    parser.add_argument("--keep-on-failure", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    print(f"NOTICE: {REFERENCE_NOTICE}", file=sys.stderr)
    args = parse_args(argv)
    root = Path(tempfile.mkdtemp(prefix="agentbenchmark-aws-smoke-")).resolve()
    resources: dict[str, Any] = {}
    try:
        resources = create_resources(args, root, resources)
        result = run_remote_smoke(args, resources)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    finally:
        if resources and not args.keep_on_failure:
            cleanup(args, resources)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
