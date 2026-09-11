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
    """Copy the base root without its S3 backend for offline graph inspection."""
    graph_root = tmp_path_factory.mktemp("terraform-graph") / "base"
    shutil.copytree(TERRAFORM_ROOTS[0], graph_root)

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
