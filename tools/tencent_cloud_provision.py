#!/usr/bin/env python3
"""Unsupported reference helper for Tencent Cloud resource lifecycle.

Automatic cloud resource creation and deletion are outside the supported
project scope. This file is retained for historical validation and community
reference; maintained provisioning support requires a contributor-owned PR.

Creates a configurable controller plus any number of worker CVMs and writes:

- `resources.json`: cloud resource IDs for cleanup.
- `inventory.json`: input for `tools/tencent_cloud_matrix.py`.

It never reuses an existing running instance. Cleanup is explicit.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import ipaddress
import json
import os
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


REFERENCE_NOTICE = (
    "cloud resource lifecycle is outside the supported project scope; "
    "this Tencent provisioner is retained as an unsupported reference, and "
    "maintained support requires a contributor-owned PR"
)


DEFAULT_CONTROLLER_TYPE = "SA2.MEDIUM2"
DEFAULT_WORKER_TYPES = [
    "SA2.MEDIUM2",
    "SA3.MEDIUM4",
    "SA2.MEDIUM8",
    "S6.LARGE8",
    "SA5.2XLARGE16",
]
DEFAULT_WORKER_MAX_CONCURRENCIES = [16, 32, 48, 96, 160]
DEFAULT_WORKER_SYSTEM_DISK_TYPES = [
    "CLOUD_PREMIUM",
    "CLOUD_PREMIUM",
    "CLOUD_PREMIUM",
    "CLOUD_PREMIUM",
    "CLOUD_BSSD",
]
DEFAULT_VPC_CIDR = "10.77.0.0/16"
DEFAULT_SUBNET_CIDR = "10.77.0.0/24"
TERMINATABLE_INSTANCE_STATES = {"RUNNING", "STOPPED"}
SSH_BOOTSTRAP = """#!/bin/bash
exec >>/var/log/agentbenchmark-bootstrap.log 2>&1
set -x
ssh-keygen -A
systemctl unmask ssh.service || true
systemctl enable ssh.service || true
systemctl restart ssh.service || systemctl restart sshd.service || true
"""


def run(cmd: list[str], *, timeout: int = 180, check: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    if check and proc.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(cmd)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}")
    return proc


def tccli(args: argparse.Namespace, service: str, action: str, params: dict[str, Any], *, timeout: int = 240, check: bool = True) -> dict[str, Any]:
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", delete=False) as tmp:
        json.dump(params, tmp, ensure_ascii=False)
        tmp_path = Path(tmp.name)
    try:
        cmd = ["tccli", service, action, "--region", args.region, "--cli-input-json", f"file://{tmp_path}"]
        proc = run(cmd, timeout=timeout, check=check)
        data: dict[str, Any] = {}
        if proc.stdout.strip():
            try:
                parsed = json.loads(proc.stdout)
            except json.JSONDecodeError:
                data["_stdout"] = proc.stdout
            else:
                if isinstance(parsed, dict):
                    data = parsed
        if not check and proc.returncode != 0:
            data["_returncode"] = proc.returncode
            data["_stderr"] = proc.stderr
        return data
    finally:
        tmp_path.unlink(missing_ok=True)


def public_ip_cidr() -> str:
    # Tencent resolves this sentinel from the API request's direct source IP.
    # That stays correct when local HTTP traffic uses a transparent proxy but
    # SSH and controller traffic leave through the machine's normal route.
    return "MY_PUBLIC_IP"


def chmod_private(path: Path) -> None:
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)


def create_key_pair(args: argparse.Namespace, out_dir: Path, name: str) -> dict[str, Any]:
    data = tccli(args, "cvm", "CreateKeyPair", {"KeyName": name, "ProjectId": 0})
    key_pair = data["KeyPair"]
    key_path = out_dir / f"{name}.pem"
    key_path.write_text(key_pair["PrivateKey"], encoding="utf-8")
    chmod_private(key_path)
    return {"key_id": key_pair["KeyId"], "key_name": name, "key_path": str(key_path)}


def create_security_group(args: argparse.Namespace, name: str) -> str:
    data = tccli(
        args,
        "vpc",
        "CreateSecurityGroup",
        {"GroupName": name, "GroupDescription": "temporary AgentBenchmark Tencent matrix"},
    )
    return data["SecurityGroup"]["SecurityGroupId"]


def create_vpc(args: argparse.Namespace, name: str, cidr: str) -> str:
    data = tccli(args, "vpc", "CreateVpc", {"VpcName": name, "CidrBlock": cidr})
    return data["Vpc"]["VpcId"]


def create_subnet(args: argparse.Namespace, name: str, vpc_id: str, cidr: str) -> str:
    data = tccli(
        args,
        "vpc",
        "CreateSubnet",
        {"VpcId": vpc_id, "SubnetName": name, "CidrBlock": cidr, "Zone": args.zone},
    )
    return data["Subnet"]["SubnetId"]


def write_resources(out_dir: Path, resources: dict[str, Any]) -> None:
    (out_dir / "resources.json").write_text(json.dumps(resources, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def initial_resources(
    args: argparse.Namespace,
    prefix: str,
    ssh_cidr: str,
    controller_cidr: str,
    worker_cidr: str,
    direct_worker_cidr: str | None,
    vpc_cidr: str,
    subnet_cidr: str,
) -> dict[str, Any]:
    return {
        "region": args.region,
        "zone": args.zone,
        "prefix": prefix,
        "controller_mode": args.controller_mode,
        "instance_charge_type": args.instance_charge_type,
        "key": {},
        "security_group_id": None,
        "network": {
            "vpc_id": args.vpc_id,
            "subnet_id": args.subnet_id,
            "vpc_cidr": vpc_cidr,
            "subnet_cidr": subnet_cidr,
            "created_vpc": False,
            "created_subnet": False,
        },
        "ssh_cidr": ssh_cidr,
        "controller_cidr": controller_cidr,
        "worker_cidr": worker_cidr,
        "direct_worker_cidr": direct_worker_cidr,
        "controller_port": args.controller_port,
        "worker_command_port": args.worker_command_port,
        "instance_ids": [],
    }


def set_security_group(resources: dict[str, Any], sg_id: str) -> None:
    resources["security_group_id"] = sg_id


def expand_per_worker(values: list[Any], count: int, *, default: Any, label: str) -> list[Any]:
    if not values:
        return [default for _ in range(count)]
    if len(values) == 1:
        return values * count
    if len(values) != count:
        raise ValueError(f"{label} must be supplied once or exactly {count} times")
    return values


def security_group_policies(
    ssh_cidr: str,
    controller_cidr: str,
    worker_cidr: str,
    direct_worker_cidr: str | None,
    controller_port: int,
    worker_command_port: int,
) -> dict[str, list[dict[str, Any]]]:
    ingress = [
        {"Protocol": "TCP", "Port": "22", "CidrBlock": ssh_cidr, "Action": "ACCEPT", "PolicyDescription": "temporary ssh"},
        {"Protocol": "TCP", "Port": str(controller_port), "CidrBlock": controller_cidr, "Action": "ACCEPT", "PolicyDescription": "controller api from operator"},
        {"Protocol": "TCP", "Port": str(controller_port), "CidrBlock": worker_cidr, "Action": "ACCEPT", "PolicyDescription": "controller api from workers"},
    ]
    if direct_worker_cidr:
        ingress.append(
            {
                "Protocol": "TCP",
                "Port": str(worker_command_port),
                "CidrBlock": direct_worker_cidr,
                "Action": "ACCEPT",
                "PolicyDescription": "direct worker api from operator",
            }
        )
    return {
        "Ingress": ingress,
        "Egress": [
            {"Protocol": "ALL", "Port": "ALL", "CidrBlock": "0.0.0.0/0", "Action": "ACCEPT", "PolicyDescription": "egress"}
        ],
    }


def configure_security_group(args: argparse.Namespace, sg_id: str, policies: dict[str, list[dict[str, Any]]]) -> None:
    for direction in ("Ingress", "Egress"):
        tccli(
            args,
            "vpc",
            "CreateSecurityGroupPolicies",
            {"SecurityGroupId": sg_id, "SecurityGroupPolicySet": {direction: policies[direction]}},
        )


def run_instance(
    args: argparse.Namespace,
    *,
    name: str,
    instance_type: str,
    disk_type: str,
    key_id: str,
    sg_id: str,
) -> str:
    payload = {
        "InstanceChargeType": args.instance_charge_type,
        "Placement": {"Zone": args.zone},
        "InstanceType": instance_type,
        "ImageId": args.image_id,
        "SystemDisk": {"DiskType": disk_type, "DiskSize": args.system_disk_size},
        "VirtualPrivateCloud": {"VpcId": args.vpc_id, "SubnetId": args.subnet_id},
        "InternetAccessible": {
            "InternetChargeType": "TRAFFIC_POSTPAID_BY_HOUR",
            "InternetMaxBandwidthOut": args.bandwidth_mbps,
            "PublicIpAssigned": True,
        },
        "InstanceCount": 1,
        "InstanceName": name,
        "LoginSettings": {"KeyIds": [key_id]},
        "SecurityGroupIds": [sg_id],
        "ClientToken": name,
    }
    if not args.disable_ssh_bootstrap:
        payload["UserData"] = base64.b64encode(SSH_BOOTSTRAP.encode("utf-8")).decode("ascii")
    data = tccli(args, "cvm", "RunInstances", payload, timeout=300)
    return data["InstanceIdSet"][0]


def describe_instances(args: argparse.Namespace, instance_ids: list[str]) -> list[dict[str, Any]]:
    data = tccli(args, "cvm", "DescribeInstances", {"InstanceIds": instance_ids}, timeout=180)
    return data.get("InstanceSet") or []


def wait_running(args: argparse.Namespace, instance_ids: list[str], timeout: int = 900) -> list[dict[str, Any]]:
    deadline = time.time() + timeout
    last: list[dict[str, Any]] = []
    while time.time() < deadline:
        last = describe_instances(args, instance_ids)
        states = {item["InstanceId"]: item.get("InstanceState") for item in last}
        if len(last) == len(instance_ids) and all(states.get(i) == "RUNNING" for i in instance_ids):
            return last
        time.sleep(10)
    raise RuntimeError(f"instances did not reach RUNNING: {last}")


def write_inventory(args: argparse.Namespace, out_dir: Path, key_path: str, instances: list[dict[str, Any]], prefix: str) -> dict[str, Any]:
    by_name = {item["InstanceName"]: item for item in instances}
    workers = []
    for idx, instance_type in enumerate(args.worker_type, start=1):
        name = f"{prefix}-worker-{idx}"
        inst = by_name[name]
        workers.append(
            {
                "worker_id": name,
                "host": inst["PublicIpAddresses"][0],
                "private_host": inst["PrivateIpAddresses"][0],
                "user": args.ssh_user,
                "key_path": key_path,
                "connection_mode": args.worker_connection_mode[idx - 1],
                "instance_type": instance_type,
                "cpu": inst.get("CPU"),
                "memory_gb": inst.get("Memory"),
                "system_disk_type": args.worker_system_disk_type[idx - 1],
                "max_concurrency": args.worker_max_concurrency[idx - 1],
                "serve_port": args.worker_command_port,
                "command_port": args.worker_command_port,
                "capabilities": ["linux", "tencent", instance_type.replace(".", "-").lower()],
            }
        )
    if args.controller_mode == "ssh-start":
        controller_instance = by_name[f"{prefix}-controller"]
        controller = {
            "connection_mode": "ssh-start",
            "host": controller_instance["PublicIpAddresses"][0],
            "private_host": controller_instance["PrivateIpAddresses"][0],
            "user": args.ssh_user,
            "key_path": key_path,
            "instance_type": args.controller_type,
        }
        controller_public_url = f"http://{controller_instance['PublicIpAddresses'][0]}:{args.controller_port}"
        controller_worker_url = f"http://{controller_instance['PrivateIpAddresses'][0]}:{args.controller_port}"
    else:
        controller = {
            "connection_mode": args.controller_mode,
            "bind_host": args.local_controller_bind_host,
        }
        controller_public_url = args.controller_public_url
        controller_worker_url = args.controller_worker_url or controller_public_url
    inventory = {
        "connection_defaults": {
            "ssh_control_persist": args.ssh_control_persist,
        },
        "controller": controller,
        "controller_public_url": controller_public_url,
        "controller_worker_url": controller_worker_url,
        "controller_private_url": controller_worker_url,
        "workers": workers,
    }
    (out_dir / "inventory.json").write_text(json.dumps(inventory, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return inventory


def cmd_create(args: argparse.Namespace) -> int:
    out_dir = args.output_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    using_default_workers = not args.worker_type
    if using_default_workers:
        args.worker_type = list(DEFAULT_WORKER_TYPES)
    worker_count = len(args.worker_type)
    if worker_count < 1:
        raise ValueError("at least one --worker-type is required")
    if using_default_workers and not args.worker_max_concurrency:
        args.worker_max_concurrency = list(DEFAULT_WORKER_MAX_CONCURRENCIES)
    args.worker_max_concurrency = [
        int(value)
        for value in expand_per_worker(
            args.worker_max_concurrency,
            worker_count,
            default=32,
            label="--worker-max-concurrency",
        )
    ]
    args.worker_connection_mode = [
        str(value)
        for value in expand_per_worker(
            args.worker_connection_mode,
            worker_count,
            default="long-poll",
            label="--worker-connection-mode",
        )
    ]
    if using_default_workers and not args.worker_system_disk_type:
        args.worker_system_disk_type = list(DEFAULT_WORKER_SYSTEM_DISK_TYPES)
    args.worker_system_disk_type = [
        str(value)
        for value in expand_per_worker(
            args.worker_system_disk_type,
            worker_count,
            default=args.system_disk_type,
            label="--worker-system-disk-type",
        )
    ]
    if args.controller_mode == "local-process":
        args.controller_public_url = args.controller_public_url or f"http://127.0.0.1:{args.controller_port}"
        if not args.controller_worker_url:
            raise ValueError("local-process controller with cloud workers requires --controller-worker-url")
    elif args.controller_mode == "prestarted":
        if not args.controller_public_url:
            raise ValueError("prestarted controller requires --controller-public-url")
        args.controller_worker_url = args.controller_worker_url or args.controller_public_url
    if bool(args.vpc_id) != bool(args.subnet_id):
        raise ValueError("--vpc-id and --subnet-id must be supplied together")
    create_network = not args.vpc_id
    if not create_network and not (args.vpc_cidr or args.worker_cidr):
        raise ValueError("existing networks require --vpc-cidr or --worker-cidr for private controller access")
    vpc_cidr = args.vpc_cidr or (DEFAULT_VPC_CIDR if create_network else args.worker_cidr)
    subnet_cidr = args.subnet_cidr or (DEFAULT_SUBNET_CIDR if create_network else "")
    if create_network:
        vpc_network = ipaddress.ip_network(vpc_cidr)
        subnet_network = ipaddress.ip_network(subnet_cidr)
        if not subnet_network.subnet_of(vpc_network):
            raise ValueError(f"subnet CIDR {subnet_cidr} is outside VPC CIDR {vpc_cidr}")
    prefix = args.name_prefix or "ab" + dt.datetime.now(dt.timezone.utc).strftime("%m%d%H%M%S")
    ssh_cidr = args.ssh_cidr or public_ip_cidr()
    controller_cidr = args.controller_cidr or ssh_cidr
    worker_cidr = args.worker_cidr or vpc_cidr or controller_cidr
    direct_worker_cidr = args.direct_worker_cidr
    if direct_worker_cidr is None and "direct-worker-api" in args.worker_connection_mode:
        direct_worker_cidr = ssh_cidr
    resources = initial_resources(
        args,
        prefix,
        ssh_cidr,
        controller_cidr,
        worker_cidr,
        direct_worker_cidr,
        vpc_cidr,
        subnet_cidr,
    )
    resources["worker_count"] = worker_count
    resources["controller_system_disk_type"] = args.system_disk_type
    resources["worker_system_disk_types"] = args.worker_system_disk_type
    resources["network"]["managed"] = create_network
    write_resources(out_dir, resources)
    try:
        network = resources["network"]
        if create_network:
            args.vpc_id = create_vpc(args, prefix, vpc_cidr)
            network["vpc_id"] = args.vpc_id
            network["created_vpc"] = True
            write_resources(out_dir, resources)
            args.subnet_id = create_subnet(args, prefix, args.vpc_id, subnet_cidr)
            network["subnet_id"] = args.subnet_id
            network["created_subnet"] = True
            write_resources(out_dir, resources)
        key = create_key_pair(args, out_dir, prefix)
        resources["key"] = key
        write_resources(out_dir, resources)
        sg_id = create_security_group(args, prefix)
        set_security_group(resources, sg_id)
        write_resources(out_dir, resources)
        configure_security_group(
            args,
            sg_id,
            security_group_policies(
                ssh_cidr,
                controller_cidr,
                worker_cidr,
                direct_worker_cidr,
                args.controller_port,
                args.worker_command_port,
            ),
        )
        instance_ids = resources["instance_ids"]
        if args.controller_mode == "ssh-start":
            instance_ids.append(
                run_instance(
                    args,
                    name=f"{prefix}-controller",
                    instance_type=args.controller_type,
                    disk_type=args.system_disk_type,
                    key_id=key["key_id"],
                    sg_id=sg_id,
                )
            )
            write_resources(out_dir, resources)
        for idx, instance_type in enumerate(args.worker_type, start=1):
            instance_ids.append(
                run_instance(
                    args,
                    name=f"{prefix}-worker-{idx}",
                    instance_type=instance_type,
                    disk_type=args.worker_system_disk_type[idx - 1],
                    key_id=key["key_id"],
                    sg_id=sg_id,
                )
            )
            write_resources(out_dir, resources)
        instances = wait_running(args, instance_ids)
        inventory = write_inventory(args, out_dir, key["key_path"], instances, prefix)
        print(json.dumps({"ok": True, "resources": resources, "inventory": inventory}, ensure_ascii=False, indent=2))
        return 0
    except Exception:
        write_resources(out_dir, resources)
        raise


def operation_ok(data: dict[str, Any]) -> bool:
    return data.get("_returncode") is None


def operation_summary(data: dict[str, Any]) -> dict[str, Any]:
    if operation_ok(data):
        return {"ok": True}
    return {
        "ok": False,
        "returncode": data.get("_returncode"),
        "error": str(data.get("_stderr") or data.get("_stdout") or "unknown tccli error")[-2000:],
    }


def retry_delete(
    args: argparse.Namespace,
    service: str,
    action: str,
    params: dict[str, Any],
    *,
    attempts: int = 24,
) -> dict[str, Any]:
    last: dict[str, Any] = {}
    for attempt in range(1, attempts + 1):
        last = tccli(args, service, action, params, check=False)
        if operation_ok(last):
            return {"ok": True, "attempts": attempt}
        if attempt < attempts:
            time.sleep(5)
    return {**operation_summary(last), "attempts": attempts}


def paged_collection(
    args: argparse.Namespace,
    service: str,
    action: str,
    collection_key: str,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    items: list[dict[str, Any]] = []
    offset = 0
    while True:
        page_params: dict[str, Any]
        if service == "vpc":
            page_params = {"Offset": str(offset), "Limit": "100"}
        else:
            page_params = {"Offset": offset, "Limit": 100}
        data = tccli(args, service, action, page_params, check=False)
        if not operation_ok(data):
            return items, operation_summary(data)
        page = data.get(collection_key) or []
        items.extend(item for item in page if isinstance(item, dict))
        total = safe_int(data.get("TotalCount"), len(items))
        if not page or len(items) >= total or len(page) < 100:
            return items, None
        offset += len(page)


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def discover_prefixed_instances(
    args: argparse.Namespace,
    prefix: str,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    instances, error = paged_collection(args, "cvm", "DescribeInstances", "InstanceSet")
    return [item for item in instances if str(item.get("InstanceName") or "").startswith(prefix + "-")], error


def cleanup_prefixed_instances(
    args: argparse.Namespace,
    prefix: str,
    recorded_ids: list[str],
    *,
    timeout: int = 900,
) -> dict[str, Any]:
    deadline = time.time() + timeout
    known_ids = set(str(item) for item in recorded_ids)
    requests: list[dict[str, Any]] = []
    last_states: dict[str, str] = {}
    while time.time() < deadline:
        instances, error = discover_prefixed_instances(args, prefix)
        if error:
            return {"ok": False, "known_ids": sorted(known_ids), "discovery_error": error}
        if not instances:
            return {"ok": True, "known_ids": sorted(known_ids), "requests": requests, "instances_absent": True}
        known_ids.update(str(item.get("InstanceId")) for item in instances if item.get("InstanceId"))
        last_states = {str(item.get("InstanceId")): str(item.get("InstanceState") or "") for item in instances}
        ready = [instance_id for instance_id, state in last_states.items() if state in TERMINATABLE_INSTANCE_STATES]
        if ready:
            result = tccli(args, "cvm", "TerminateInstances", {"InstanceIds": ready}, check=False)
            requests.append({"instance_ids": ready, **operation_summary(result)})
        time.sleep(10)
    return {
        "ok": False,
        "known_ids": sorted(known_ids),
        "instances_absent": False,
        "last_states": last_states,
        "requests": requests,
    }


def discover_named_ids(
    args: argparse.Namespace,
    service: str,
    action: str,
    collection_key: str,
    id_key: str,
    name_key: str,
    prefix: str,
) -> tuple[list[str], dict[str, Any] | None]:
    items, error = paged_collection(args, service, action, collection_key)
    ids = [
        str(item[id_key])
        for item in items
        if item.get(id_key) and str(item.get(name_key) or "") == prefix
    ]
    return ids, error


def cleanup_named_resources(
    args: argparse.Namespace,
    *,
    service: str,
    describe_action: str,
    collection_key: str,
    id_key: str,
    name_key: str,
    delete_action: str,
    delete_param: str,
    prefix: str,
    recorded_ids: list[str],
) -> dict[str, Any]:
    discovered, error = discover_named_ids(
        args,
        service,
        describe_action,
        collection_key,
        id_key,
        name_key,
        prefix,
    )
    if error:
        targets = sorted(set(recorded_ids))
    else:
        targets = sorted(set(discovered))
    attempts: dict[str, Any] = {}
    for resource_id in targets:
        value: Any = [resource_id] if delete_param.endswith("Ids") else resource_id
        attempts[resource_id] = retry_delete(args, service, delete_action, {delete_param: value})
    remaining: list[str] = targets
    verification_error: dict[str, Any] | None = error
    for _ in range(12):
        remaining, verification_error = discover_named_ids(
            args,
            service,
            describe_action,
            collection_key,
            id_key,
            name_key,
            prefix,
        )
        if verification_error or not remaining:
            break
        time.sleep(5)
    ok = verification_error is None and not remaining
    return {
        "ok": ok,
        "targets": targets,
        "attempts": attempts,
        "remaining": remaining,
        "verification_error": verification_error,
    }


def cmd_cleanup(args: argparse.Namespace) -> int:
    resources = json.loads(args.resources.read_text(encoding="utf-8-sig"))
    args.region = resources["region"]
    prefix = str(resources.get("prefix") or "")
    if not prefix:
        raise ValueError("resources.json is missing prefix; refusing discovery-based cleanup")
    instance_ids = [str(item) for item in resources.get("instance_ids") or []]
    cleanup: dict[str, Any] = {"prefix": prefix, "recorded_instance_ids": instance_ids}
    errors: list[str] = []
    cleanup["instances"] = cleanup_prefixed_instances(args, prefix, instance_ids)
    if not cleanup["instances"]["ok"]:
        errors.append("failed to terminate all prefixed instances")
    sg_id = str(resources.get("security_group_id") or "")
    cleanup["security_groups"] = cleanup_named_resources(
        args,
        service="vpc",
        describe_action="DescribeSecurityGroups",
        collection_key="SecurityGroupSet",
        id_key="SecurityGroupId",
        name_key="SecurityGroupName",
        delete_action="DeleteSecurityGroup",
        delete_param="SecurityGroupId",
        prefix=prefix,
        recorded_ids=[sg_id] if sg_id else [],
    )
    if not cleanup["security_groups"]["ok"]:
        errors.append("failed to delete all prefixed security groups")
    key = resources.get("key") or {}
    key_id = str(key.get("key_id") or "")
    cleanup["keys"] = cleanup_named_resources(
        args,
        service="cvm",
        describe_action="DescribeKeyPairs",
        collection_key="KeyPairSet",
        id_key="KeyId",
        name_key="KeyName",
        delete_action="DeleteKeyPairs",
        delete_param="KeyIds",
        prefix=prefix,
        recorded_ids=[key_id] if key_id else [],
    )
    if not cleanup["keys"]["ok"]:
        errors.append("failed to delete all prefixed keys")
    if key.get("key_path"):
        Path(key["key_path"]).unlink(missing_ok=True)
    (args.resources.parent / f"{prefix}.pem").unlink(missing_ok=True)
    network = resources.get("network") or {}
    managed_network = bool(network.get("managed") or network.get("created_vpc") or network.get("created_subnet"))
    if managed_network:
        subnet_id = str(network.get("subnet_id") or "")
        cleanup["subnets"] = cleanup_named_resources(
            args,
            service="vpc",
            describe_action="DescribeSubnets",
            collection_key="SubnetSet",
            id_key="SubnetId",
            name_key="SubnetName",
            delete_action="DeleteSubnet",
            delete_param="SubnetId",
            prefix=prefix,
            recorded_ids=[subnet_id] if subnet_id else [],
        )
        if not cleanup["subnets"]["ok"]:
            errors.append("failed to delete all prefixed subnets")
        vpc_id = str(network.get("vpc_id") or "")
        cleanup["vpcs"] = cleanup_named_resources(
            args,
            service="vpc",
            describe_action="DescribeVpcs",
            collection_key="VpcSet",
            id_key="VpcId",
            name_key="VpcName",
            delete_action="DeleteVpc",
            delete_param="VpcId",
            prefix=prefix,
            recorded_ids=[vpc_id] if vpc_id else [],
        )
        if not cleanup["vpcs"]["ok"]:
            errors.append("failed to delete all prefixed VPCs")
    cleanup["errors"] = errors
    cleanup["ok"] = not errors
    print(json.dumps(cleanup, ensure_ascii=False, indent=2))
    return 0 if not errors else 1


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Unsupported reference helper for temporary Tencent Cloud matrix hosts.",
        epilog=REFERENCE_NOTICE,
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("create")
    p.add_argument("--region", default="ap-guangzhou")
    p.add_argument("--zone", required=True)
    p.add_argument("--vpc-id", default=None)
    p.add_argument("--subnet-id", default=None)
    p.add_argument("--vpc-cidr", default=None)
    p.add_argument("--subnet-cidr", default=None)
    p.add_argument("--image-id", required=True)
    p.add_argument("--controller-type", default=DEFAULT_CONTROLLER_TYPE)
    p.add_argument("--controller-mode", choices=["ssh-start", "prestarted", "local-process"], default="ssh-start")
    p.add_argument("--controller-public-url", default=None)
    p.add_argument("--controller-worker-url", default=None)
    p.add_argument("--local-controller-bind-host", default="127.0.0.1")
    p.add_argument("--instance-charge-type", choices=["SPOTPAID", "POSTPAID_BY_HOUR"], default="SPOTPAID")
    p.add_argument("--worker-type", action="append", default=[])
    p.add_argument("--worker-max-concurrency", type=int, action="append", default=[])
    p.add_argument("--worker-connection-mode", choices=["ssh-start", "long-poll", "direct-worker-api"], action="append", default=[])
    p.add_argument("--worker-command-port", type=int, default=9876)
    p.add_argument("--controller-port", type=int, default=8765)
    p.add_argument("--ssh-control-persist", default="10m")
    p.add_argument("--system-disk-type", default="CLOUD_PREMIUM")
    p.add_argument("--worker-system-disk-type", action="append", default=[])
    p.add_argument("--system-disk-size", type=int, default=20)
    p.add_argument("--bandwidth-mbps", type=int, default=1)
    p.add_argument("--ssh-user", default="ubuntu")
    p.add_argument("--disable-ssh-bootstrap", action="store_true")
    p.add_argument("--ssh-cidr", default=None)
    p.add_argument("--controller-cidr", default=None)
    p.add_argument("--worker-cidr", default=None)
    p.add_argument("--direct-worker-cidr", default=None)
    p.add_argument("--name-prefix", default=None)
    p.add_argument("--output-dir", type=Path, required=True)
    p.set_defaults(func=cmd_create)

    p = sub.add_parser("cleanup")
    p.add_argument("--region", default="ap-guangzhou")
    p.add_argument("--resources", type=Path, required=True)
    p.set_defaults(func=cmd_cleanup)

    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    print(f"NOTICE: {REFERENCE_NOTICE}", file=sys.stderr)
    args = parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
