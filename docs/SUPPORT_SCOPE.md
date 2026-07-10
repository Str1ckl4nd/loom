# Support Scope

AgentBenchmark Control Worker starts at the host boundary. Operators provide an
inventory of controller and worker hosts; the project deploys or connects to
processes on those hosts and manages benchmark work from there.

## Supported

- inventory-driven registration of existing controller and worker hosts;
- `ssh-start`, `long-poll`, and `direct-worker-api` worker connections;
- remote, prestarted, or explicitly configured local controller placement;
- task normalization, dispatch, leases, adaptive concurrency, retries, logs,
  result upload, querying, and recovery;
- remote validation against hosts supplied by the operator.

## Out Of Scope

Automatic cloud resource lifecycle is not a supported feature and is not on the
project roadmap. This includes:

- creating, resizing, stopping, or deleting virtual machines;
- selecting providers, regions, zones, instance types, images, or prices;
- creating VPCs, subnets, security groups, cloud SSH keys, public IPs, or other
  provider infrastructure;
- managing provider credentials, quotas, billing, cost controls, or teardown.

The operator or an external infrastructure system owns those responsibilities.
The supported cloud workflow begins after hosts exist and an `inventory.json`
can be supplied to `tools/tencent_cloud_matrix.py` or another inventory-driven
runner.

## Retained Reference Helpers

The existing `tools/tencent_cloud_provision.py`, `tools/tencent_cloud_e2e.py`,
and `tools/aws_linux_control_plane_smoke.py` files are retained as historical
validation and community reference implementations. They are not supported
interfaces, carry no compatibility or maintenance commitment, and must not be
treated as the project's resource lifecycle layer.

Anyone who needs maintained automatic provisioning support should propose and
own it through a pull request, including provider-specific tests, security and
cost boundaries, failure recovery, and cleanup behavior.
