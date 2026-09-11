# Terraform state bootstrap

This root creates the private S3 bucket and grants its narrowly scoped state
permissions to the existing Terraform operator used by the `base` root. Its state
stays local because a backend cannot create the bucket that holds its own state.
Keep that state encrypted on the workstation, outside synchronized folders, and
backed up with restricted file permissions. The repository ignores state, plans,
real variable files, backend configuration, and Terraform data directories.

## Offline checks

Always initialize checks with a separate Terraform data directory and the real
backend disabled:

```shell
TF_DATA_DIR=.terraform-offline/state-bootstrap \
  terraform -chdir=infra/state-bootstrap init -backend=false -input=false
TF_DATA_DIR=.terraform-offline/state-bootstrap \
  terraform -chdir=infra/state-bootstrap test
```

These commands validate configuration only. Applying either root requires a
separately approved AWS session.

## Initial migration and verification

The base root already declares its S3 backend, so `terraform state pull` and
`terraform state list` in that directory require backend initialization. Before
the first migration, do not use those commands against the base root. Stop all
Terraform processes and copy the existing `infra/base/terraform.tfstate` file
directly to a private directory outside the repository. Use a restricted copy,
for example:

```shell
install -m 600 infra/base/terraform.tfstate \
  /secure/private/carrier-risk-state/base-before-remote.tfstate
```

Replace the example destination with an existing encrypted directory owned only
by the operator. From a directory containing no `.tf` files, record the backup's
checksum, lineage, serial, and resource addresses without loading the base
configuration:

```shell
shasum -a 256 /secure/private/carrier-risk-state/base-before-remote.tfstate
jq -r '.lineage, .serial' \
  /secure/private/carrier-risk-state/base-before-remote.tfstate
terraform state list \
  -state=/secure/private/carrier-risk-state/base-before-remote.tfstate
```

After the backup is verified, prepare the ignored bootstrap `terraform.tfvars`
from `terraform.tfvars.example`, specifying the existing deployment role. During
the approved AWS session, initialize and apply `infra/state-bootstrap` from its
secured local state. This must succeed before initializing the S3 backend: the
bucket and state-access policy must already exist. Use its `state_bucket_name`
output for the backend configuration.

Give every independently managed bootstrap/base stack a unique `environment`
value, and use the same value in both roots. The environment scopes the inline
remote-state policy on the shared operator role, so separate stacks cannot
replace or delete one another's policy during apply or destroy. Independent
state buckets must also retain distinct bucket prefixes.

Then copy `infra/base/backend.hcl.example` to the ignored
`infra/base/backend.hcl`, replace its bucket placeholder, and initialize the base
root with `-migrate-state -backend-config=backend.hcl`. Use `umask 077` before
pulling the migrated remote state to another private backup, then compare its
checksum, lineage, serial, and resource-address list using the same backend-free
inspection commands. Lineage and every resource address must be unchanged; the
serial may advance during migration.

The project uses only Terraform's default workspace. The state policy deliberately
does not grant access to the `env:/` discovery prefix used by CLI workspaces; keep
the single `base` root instead of creating Terraform workspaces.

Test native locking with two terminals under the deployment identity. Keep
`terraform console` open in the first terminal so it owns the lock. A
`terraform plan -lock-timeout=0s` in the second terminal must fail with lock
metadata. Exit the console normally, then repeat the plan; it must acquire and
release the lock successfully. Record only redacted evidence: state bucket names,
role ARNs, account ids, endpoints, and lock metadata are sensitive operational
details.

If a process dies while holding the lock, first prove that no Terraform process or
approved deployment is active. Preserve a fresh state backup, record the stale lock
id privately, and use `terraform force-unlock` under the deployment identity. Never
delete the state object to clear a lock. Repeat the normal plan to prove recovery.

## Final removal

Final cleanup is intentionally ordered and separately approved:

1. Destroy all resources managed by the `base` root while its S3 backend is intact.
   This also deletes every ECR image because the repository is a disposable
   application artifact with `force_delete = true`.
2. Pull and secure the final empty state. Copy the initialized base working
   directory and its Terraform data directory to a private temporary location,
   change that copy's backend to `local`, and run `terraform init -migrate-state`;
   verify lineage and the empty resource-address list again.
3. Remove every version and delete marker for the remote state and lock objects
   under the approved cleanup procedure.
4. Destroy the bootstrap root from its secured local state, then remove its local
   state only after the bucket and state policy are confirmed absent.

The bucket sets `force_destroy = false`, so Terraform cannot silently bypass this
order or discard version history during an ordinary destroy.
