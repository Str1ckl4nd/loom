# Loom Scope

Loom starts at the host boundary. Operators provide an inventory of Loom Hub and
Loom Runner hosts; the project deploys or connects to processes on those hosts
and manages evaluation work from there.

## Supported

- inventory-driven registration of existing Loom Hub and Loom Runner hosts;
- `ssh-start`, `long-poll`, and `direct-worker-api` Runner connections;
- remote, prestarted, or explicitly configured local Hub placement;
- task normalization, dispatch, leases, fixed/adaptive concurrency policies, retries, logs,
  result upload, querying, and recovery;
- remote validation against hosts supplied by the operator, including the
  process-level cleanup performed by `loom_agentdojo_remote_smoke.py`.

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
can be supplied to `tools/loom_matrix.py` or another inventory-driven
runner. Loom's remote smoke helper stops only the Hub and Runner processes it
started; explicit VM stop/delete and billing verification remain an operator
workflow step.

## Retained Reference Helpers

The existing `tools/loom_tencent_provision_reference.py`, `tools/loom_tencent_e2e_reference.py`,
and `tools/loom_aws_smoke_reference.py` files are retained as historical
validation and community reference implementations. They are not supported
interfaces, carry no compatibility or maintenance commitment, and must not be
treated as the project's resource lifecycle layer.

Anyone who needs maintained automatic provisioning support should propose and
own it through a pull request, including provider-specific tests, security and
cost boundaries, failure recovery, and cleanup behavior.
