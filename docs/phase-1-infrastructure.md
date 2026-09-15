# Phase 1 infrastructure operator guide

The Phase 1 event spine is an optional module inside the single
`infra/base` Terraform root. Its default, `enable_spine = false`, retains the
Phase 0 immutable raw S3 bucket while removing the RDS warehouse, SQS queues,
loader role, and their small isolated VPC.

The module was exercised in a short-lived AWS acceptance run on 2026-09-11 and
then disabled. The run proved S3 notifications, SQS consumption, retry handling,
TLS-verified RDS loading, dbt builds, temporal corrections, and repeatable
backfills. The teardown removed all 19 module resources while preserving the
Phase 0 raw bucket and ingest role. See
[the verification report](phase-1-verification.md) for sanitized evidence.

Routine Terraform tests still use a mock AWS provider and no credentials. They
do not replace the recorded acceptance run, and running them cannot create AWS
resources.

## Permission and cost gate

Do not run an AWS-backed `plan`, `apply`, secret read, or teardown verification
until the account-level AWS Budget has been checked and the operator has given
explicit permission for that operation. RDS is the continuously billable
resource. An approved working session must end by disabling the spine and
verifying that its RDS and SQS resources are gone.

Local checks are safe and require no AWS credentials:

```sh
terraform -chdir=infra/base init
terraform -chdir=infra/base fmt -check -recursive
terraform -chdir=infra/base validate
terraform -chdir=infra/base test
```

`terraform test` uses the mock provider definitions under `infra/base/tests`.
It must not be replaced with a credentialed test.

## Review and enable the private spine

After receiving permission, create a saved plan and review every create action:

```sh
terraform -chdir=infra/base plan \
  -var='enable_spine=true' \
  -out=phase-1-enable.tfplan
terraform -chdir=infra/base show phase-1-enable.tfplan
terraform -chdir=infra/base apply phase-1-enable.tfplan
```

The default plan creates these resources:

- an encrypted SQS arrival queue and encrypted dead-letter queue.
- manifest-only S3 notifications from the existing raw bucket.
- a separate least-privilege loader role.
- one VPC and two isolated subnets, with no NAT gateway or internet route.
- one encrypted, single-AZ PostgreSQL 17 `db.t4g.micro` instance with 20 GiB of
  bounded gp3 storage and an RDS-managed master password.

Private mode has no general database ingress. It is suitable when Airflow can
reach the VPC through an approved private path. Locally run Airflow cannot reach
the RDS endpoint over the public internet in this mode.

Queue and connection metadata can be read after apply:

```sh
terraform -chdir=infra/base output -raw arrival_queue_url
terraform -chdir=infra/base output -json warehouse_connection
```

The connection output does not contain a password and is marked sensitive to
avoid casual display. After separate approval to retrieve credentials, the
native operator can read the RDS-managed secret:

```sh
secret_arn="$(terraform -chdir=infra/base output -raw warehouse_master_secret_arn)"
aws secretsmanager get-secret-value \
  --secret-id "$secret_arn" \
  --query SecretString \
  --output text
```

Treat that terminal output as a credential. Do not save it in Terraform input,
shell history, repository files, or screenshots.

## Temporary public access for local Airflow

Public connectivity is an explicit exception, not a convenience default. It
requires one developer IPv4 `/32`. Broader or IPv6 CIDRs are rejected. Determine
the reviewed address out of band, then plan both switches together:

```sh
terraform -chdir=infra/base plan \
  -var='enable_spine=true' \
  -var='warehouse_publicly_accessible=true' \
  -var='developer_ipv4_cidr=203.0.113.7/32' \
  -out=phase-1-public-enable.tfplan
terraform -chdir=infra/base show phase-1-public-enable.tfplan
terraform -chdir=infra/base apply phase-1-public-enable.tfplan
```

Replace the documentation address with the approved developer host. This mode
adds an internet gateway and opens only TCP 5432 from that `/32`. RDS supports
TLS, so the Airflow connection must require TLS certificate validation. TLS
protects traffic but does not make an internet-routable endpoint private. Disable
the spine when the working session ends instead of leaving public access in
place.

## Disable the spine and retain raw evidence

Use the same root with the gate set to false. Review that the plan deletes the
module resources but does not delete `aws_s3_bucket.raw` or its supporting
configuration:

```sh
terraform -chdir=infra/base plan \
  -var='enable_spine=false' \
  -out=phase-1-disable.tfplan
terraform -chdir=infra/base show phase-1-disable.tfplan
terraform -chdir=infra/base apply phase-1-disable.tfplan
```

Then verify both Terraform state and AWS inventory after receiving permission to
perform the AWS reads:

```sh
terraform -chdir=infra/base state list
aws rds describe-db-instances \
  --query "DBInstances[?starts_with(DBInstanceIdentifier, 'carrier-risk-warehouse-')].[DBInstanceIdentifier,DBInstanceStatus]" \
  --output table
aws sqs list-queues \
  --queue-name-prefix carrier-risk-arrival- \
  --output table
```

The state list should contain the Phase 0 S3 and ingest resources and no
`module.spine` addresses. The two AWS inventory commands should return no
matching Phase 1 database or queues. Do not claim teardown until those live
checks have actually been run and recorded. Full destruction of the base root,
including the immutable raw bucket, remains the separately reviewed Phase 2
proof and is not the Phase 1 working-session teardown.

## Recovery model

The warehouse has no deletion protection and does not create a final snapshot.
That is deliberate: it is reconstructed by replaying validated immutable source
snapshots from the retained raw S3 bucket. Before disabling the spine, confirm
that any source acquisition expected to survive is complete and represented by
its manifest. Database-only changes are not a recovery source.
