# Terraform state bootstrap

This root creates the private S3 bucket and grants its narrowly scoped state
permissions to an existing project-owned IAM role. The role may be separate from
the operator that provisions the `base` root. Bootstrap state stays local because
a backend cannot create the bucket that holds its own state.
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
from `terraform.tfvars.example`, specifying the existing state-access role. During
the approved AWS session, initialize and apply `infra/state-bootstrap` from its
secured local state. This must succeed before initializing the S3 backend: the
bucket and state-access policy must already exist. Use its `state_bucket_name`
output for the backend configuration.

Here, `terraform_operator_role_name` selects the role whose inline state policy
this root manages. The AWS provider uses the credentials supplied to Terraform
to create resources. Choose a role whose policies the project is authorized to
manage; organization-managed or SSO-managed login roles may be protected from
policy changes.

For a separate state-access role, create a project-owned role in a separately
managed Terraform root before applying bootstrap. Keep that root's state local
with the same protections as bootstrap state. Scope the role's trust to the
verified stable IAM role ARN of the deployment login, including any role path.
This bootstrap root then grants access only to its state bucket, exact state
object, and lockfile. The state role needs no resource-provisioning permissions.
Creating and assuming it must remain allowed by the account's existing policies.

Configure the base S3 backend's `assume_role` object using
`terraform_deployment_role_arn`; a commented example is in
`infra/base/backend.hcl.example`. Backend authentication is independent of the
AWS provider, so keep the provider's deployment credentials and the base root's
`terraform_operator_role_name` runtime-trust principal unchanged. When the login
already uses a role session, keep the backend session duration at most one hour,
the AWS role-chaining limit. See the [S3 backend authentication documentation](https://developer.hashicorp.com/terraform/language/backend/s3#assume-role-configuration)
and [AWS AssumeRole documentation](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html).

Give every independently managed bootstrap/base stack a unique `environment`
value, and use the same value in both roots. The environment scopes the inline
remote-state policy on the selected role, so separate stacks cannot
replace or delete one another's policy during apply or destroy. Independent
state buckets must also retain distinct bucket prefixes. Give separately managed
state roles distinct names as well, for example by including the environment.

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

Test native locking with two terminals under the deployment identity. In an
isolated acceptance root, add a temporary output with a constant value and start
`terraform apply -refresh=false -input=true`. Leave it at the confirmation prompt;
do not approve the change. A `terraform plan -lock-timeout=0s` in the second
terminal must fail with lock metadata. Answer `no` in the first terminal, remove
the temporary output, and repeat the plan; it must acquire and release the lock
successfully with no changes. Terraform 1.16.0's console did not hold a persistent
lock in the live acceptance check. Record only redacted evidence: state bucket names,
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
5. If a separate Terraform root owns the state-access role, destroy that root
   last. Retain the role and its policy until all remote-backend operations and
   the migration to local state have finished.

The bucket sets `force_destroy = false`, so Terraform cannot silently bypass this
order or discard version history during an ordinary destroy.
