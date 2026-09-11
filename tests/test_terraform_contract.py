"""Offline contract tests for the Terraform roots."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TERRAFORM_ROOTS = (
    PROJECT_ROOT / "infra" / "base",
    PROJECT_ROOT / "infra" / "state-bootstrap",
)


def run_terraform(
    terraform_root: Path,
    terraform_data_dir: Path,
    *arguments: str,
) -> subprocess.CompletedProcess[str]:
    """Run Terraform without credentials and return complete diagnostic output."""
    environment = os.environ.copy()
    for credential_name in (
        "AWS_ACCESS_KEY_ID",
        "AWS_CONFIG_FILE",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_DEFAULT_PROFILE",
        "AWS_ROLE_ARN",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SHARED_CREDENTIALS_FILE",
        "AWS_SESSION_TOKEN",
        "AWS_PROFILE",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
    ):
        environment.pop(credential_name, None)
    nonexistent_credentials = terraform_data_dir / "no-aws-credentials"
    environment["AWS_CONFIG_FILE"] = str(nonexistent_credentials)
    environment["AWS_SHARED_CREDENTIALS_FILE"] = str(nonexistent_credentials)
    environment["AWS_EC2_METADATA_DISABLED"] = "true"
    environment["TF_DATA_DIR"] = str(terraform_data_dir)

    return subprocess.run(
        ["terraform", f"-chdir={terraform_root}", *arguments],
        check=False,
        capture_output=True,
        env=environment,
        text=True,
    )


def assert_terraform_succeeded(result: subprocess.CompletedProcess[str]) -> None:
    """Fail a contract test with Terraform's full stdout and stderr."""
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.fixture(scope="module")
def initialized_terraform_roots(
    tmp_path_factory: pytest.TempPathFactory,
) -> dict[Path, Path]:
    """Initialize both roots offline without ever configuring the real backend."""
    initialized_roots: dict[Path, Path] = {}
    plugin_directory = os.environ.get("TERRAFORM_PLUGIN_DIR")

    for terraform_root in TERRAFORM_ROOTS:
        terraform_data_dir = tmp_path_factory.mktemp(terraform_root.name)
        arguments = ["init", "-backend=false", "-input=false", "-lockfile=readonly"]
        if plugin_directory:
            arguments.append(f"-plugin-dir={plugin_directory}")
        result = run_terraform(terraform_root, terraform_data_dir, *arguments)
        assert_terraform_succeeded(result)
        initialized_roots[terraform_root] = terraform_data_dir

    return initialized_roots


@pytest.fixture(scope="module")
def local_backend_graph_root(
    tmp_path_factory: pytest.TempPathFactory,
) -> tuple[Path, Path]:
    """Copy the base root and canonical schemas for offline graph inspection."""
    graph_project_root = tmp_path_factory.mktemp("terraform-graph")
    graph_root = graph_project_root / "infra" / "base"
    graph_root.mkdir(parents=True)

    terraform_sources = sorted(TERRAFORM_ROOTS[0].glob("*.tf")) + sorted(
        (TERRAFORM_ROOTS[0] / "modules").rglob("*.tf")
    )
    for source_path in terraform_sources:
        if source_path.name == "override.tf" or source_path.name.endswith(
            "_override.tf"
        ):
            continue
        destination_path = graph_root / source_path.relative_to(TERRAFORM_ROOTS[0])
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, destination_path)
    shutil.copy2(TERRAFORM_ROOTS[0] / ".terraform.lock.hcl", graph_root)

    schema_root = graph_project_root / "ingest" / "schemas"
    schema_root.mkdir(parents=True)
    for schema_name in ("crashes.json", "inspections.json"):
        shutil.copy2(PROJECT_ROOT / "ingest" / "schemas" / schema_name, schema_root)

    versions_path = graph_root / "versions.tf"
    versions_configuration = versions_path.read_text()
    local_configuration, replacements = re.subn(
        r'\n  backend "s3" \{.*?\n  \}\n',
        "\n",
        versions_configuration,
        count=1,
        flags=re.DOTALL,
    )
    assert replacements == 1
    versions_path.write_text(local_configuration)

    terraform_data_dir = tmp_path_factory.mktemp("terraform-graph-data")
    arguments = ["init", "-backend=false", "-input=false", "-lockfile=readonly"]
    if plugin_directory := os.environ.get("TERRAFORM_PLUGIN_DIR"):
        arguments.append(f"-plugin-dir={plugin_directory}")
    result = run_terraform(graph_root, terraform_data_dir, *arguments)
    assert_terraform_succeeded(result)

    return graph_root, terraform_data_dir


def test_terraform_configurations_are_formatted_and_valid(
    initialized_terraform_roots: dict[Path, Path],
) -> None:
    """Catch malformed or non-canonical Terraform before an operator plans it."""
    for terraform_root, terraform_data_dir in initialized_terraform_roots.items():
        assert_terraform_succeeded(
            run_terraform(
                terraform_root, terraform_data_dir, "fmt", "-check", "-recursive"
            )
        )
        assert_terraform_succeeded(
            run_terraform(terraform_root, terraform_data_dir, "validate", "-no-color")
        )


def test_mocked_plans_enforce_security_and_cost_contracts(
    initialized_terraform_roots: dict[Path, Path],
) -> None:
    """Exercise both roots with mock providers and no AWS access."""
    for terraform_root, terraform_data_dir in initialized_terraform_roots.items():
        assert_terraform_succeeded(
            run_terraform(terraform_root, terraform_data_dir, "test", "-no-color")
        )


def test_manifest_notification_depends_on_the_source_scoped_queue_policy(
    local_backend_graph_root: tuple[Path, Path],
) -> None:
    """Prevent S3 notification creation from racing queue-policy propagation."""
    terraform_root, terraform_data_dir = local_backend_graph_root
    result = run_terraform(terraform_root, terraform_data_dir, "graph", "-type=plan")
    assert_terraform_succeeded(result)
    expected_edge = (
        'module.spine.aws_s3_bucket_notification.raw_manifest_arrival (expand)" '
        '-> "[root] module.spine.aws_sqs_queue_policy.arrival (expand)'
    )
    assert expected_edge in result.stdout


def test_redrive_policies_depend_on_their_actual_queues(
    local_backend_graph_root: tuple[Path, Path],
) -> None:
    """Prevent constructed ARNs from hiding queue replacement dependencies."""
    terraform_root, terraform_data_dir = local_backend_graph_root
    result = run_terraform(terraform_root, terraform_data_dir, "graph", "-type=plan")
    assert_terraform_succeeded(result)
    expected_edges = (
        (
            "module.spine.aws_sqs_queue_redrive_policy.arrival (expand)",
            "module.spine.aws_sqs_queue.arrival (expand)",
        ),
        (
            "module.spine.aws_sqs_queue_redrive_policy.arrival (expand)",
            "module.spine.aws_sqs_queue.dead_letter (expand)",
        ),
        (
            "module.spine.aws_sqs_queue_redrive_allow_policy.dead_letter (expand)",
            "module.spine.aws_sqs_queue.arrival (expand)",
        ),
        (
            "module.spine.aws_sqs_queue_redrive_allow_policy.dead_letter (expand)",
            "module.spine.aws_sqs_queue.dead_letter (expand)",
        ),
    )
    for source, target in expected_edges:
        assert f'{source}" -> "[root] {target}' in result.stdout


def test_loader_trust_does_not_reuse_ingest_principals() -> None:
    """Keep snapshot-writer access separate from loader role assumption."""
    root_spine_configuration = (TERRAFORM_ROOTS[0] / "spine.tf").read_text()
    assert "var.additional_loader_principals" in root_spine_configuration
    assert "var.additional_ingest_principals" not in root_spine_configuration


def test_analytics_trust_uses_only_its_dedicated_principals() -> None:
    """Keep catalog publication and queries separate from other runtime roles."""
    iam_configuration = (TERRAFORM_ROOTS[0] / "iam.tf").read_text()
    analytics_configuration = iam_configuration.split(
        "locals {\n  analytics_principal_arns", maxsplit=1
    )[1]
    assert "var.additional_analytics_principals" in analytics_configuration
    assert "var.additional_ingest_principals" not in analytics_configuration
    assert "var.additional_loader_principals" not in analytics_configuration
    assert "var.additional_ecr_publisher_principals" not in analytics_configuration


def test_neither_runtime_role_can_delete_raw_objects_or_read_secrets() -> None:
    """Catch privilege growth across both the ingest and loader role policies."""
    iam_configuration = "\n".join(
        (TERRAFORM_ROOTS[0] / relative_path).read_text()
        for relative_path in ("iam.tf", "modules/spine/iam.tf")
    )
    assert "s3:DeleteObject" not in iam_configuration
    assert "secretsmanager:" not in iam_configuration


def test_spine_has_no_nat_gateway() -> None:
    """Prevent a paid NAT gateway from entering the small Phase 1 network."""
    spine_configuration = "\n".join(
        path.read_text()
        for path in sorted((TERRAFORM_ROOTS[0] / "modules" / "spine").glob("*.tf"))
    )
    assert 'resource "aws_nat_gateway"' not in spine_configuration


def test_runtime_roles_have_no_remote_state_access() -> None:
    """Keep application identities outside Terraform's control plane."""
    runtime_iam_configuration = "\n".join(
        path.read_text()
        for path in (
            TERRAFORM_ROOTS[0] / "iam.tf",
            TERRAFORM_ROOTS[0] / "modules" / "spine" / "iam.tf",
        )
    )
    assert "terraform.tfstate" not in runtime_iam_configuration
    assert ".tflock" not in runtime_iam_configuration


def test_analytics_infrastructure_has_no_managed_processing_services() -> None:
    """Keep the raw catalog explicit and free of paid discovery or ETL services."""
    analytics_configuration = "\n".join(
        (TERRAFORM_ROOTS[0] / relative_path).read_text()
        for relative_path in ("glue.tf", "athena.tf", "iam.tf")
    )
    forbidden_resources = (
        'resource "aws_glue_crawler"',
        'resource "aws_glue_job"',
        'resource "aws_athena_capacity_reservation"',
    )
    for forbidden_resource in forbidden_resources:
        assert forbidden_resource not in analytics_configuration


def test_athena_multipart_cleanup_covers_the_entire_results_bucket() -> None:
    """Do not restrict abandoned multipart cleanup to the query-result prefix."""
    athena_configuration = (TERRAFORM_ROOTS[0] / "athena.tf").read_text()
    expected_rule = """  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }"""
    assert expected_rule in athena_configuration


def test_graph_fixture_copies_only_canonical_schema_inputs(
    local_backend_graph_root: tuple[Path, Path],
) -> None:
    """Keep fixture schema reads valid without copying private Terraform artifacts."""
    terraform_root, _ = local_backend_graph_root
    fixture_project_root = terraform_root.parents[1]
    copied_paths = {
        path.relative_to(fixture_project_root).as_posix()
        for path in fixture_project_root.rglob("*")
        if path.is_file()
    }
    expected_terraform_paths = {
        f"infra/base/{path.relative_to(TERRAFORM_ROOTS[0]).as_posix()}"
        for path in (
            sorted(TERRAFORM_ROOTS[0].glob("*.tf"))
            + sorted((TERRAFORM_ROOTS[0] / "modules").rglob("*.tf"))
        )
        if path.name != "override.tf" and not path.name.endswith("_override.tf")
    }
    expected_paths = expected_terraform_paths | {
        "infra/base/.terraform.lock.hcl",
        "ingest/schemas/crashes.json",
        "ingest/schemas/inspections.json",
    }

    assert copied_paths == expected_paths


def test_local_terraform_artifacts_and_backend_configuration_are_ignored() -> None:
    """Prevent state, plans, credentials, and the real backend config entering git."""
    ignore_patterns = (PROJECT_ROOT / ".gitignore").read_text().splitlines()
    required_patterns = {
        ".aws/",
        "backend.hcl",
        "*.key",
        "*.pem",
        "*.tfplan",
        "*.tfstate",
        "*.tfstate.*",
        ".terraform/",
        ".terraform-offline/",
        ".terraformrc",
    }
    assert required_patterns <= set(ignore_patterns)


def test_base_backend_and_bootstrap_policy_share_one_fixed_state_key() -> None:
    """Prevent backend configuration from drifting beyond its IAM policy scope."""
    base_versions = (TERRAFORM_ROOTS[0] / "versions.tf").read_text()
    bootstrap_variables = (TERRAFORM_ROOTS[1] / "variables.tf").read_text()
    expected_state_key = '"carrier-risk/base/terraform.tfstate"'

    assert 'backend "s3"' in base_versions
    assert "use_lockfile = true" in base_versions
    assert f"key          = {expected_state_key}" in base_versions
    assert f"default     = {expected_state_key}" in bootstrap_variables


def test_bootstrap_state_remains_local() -> None:
    """Keep the state-bucket root independent from the backend it creates."""
    bootstrap_versions = (TERRAFORM_ROOTS[1] / "versions.tf").read_text()
    assert 'backend "' not in bootstrap_versions
