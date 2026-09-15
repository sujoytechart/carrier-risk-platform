"""Offline contract tests for the Phase 1 Terraform configuration."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TERRAFORM_ROOT = PROJECT_ROOT / "infra" / "base"


def run_terraform(
    terraform_data_dir: Path, *arguments: str
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
    environment["AWS_EC2_METADATA_DISABLED"] = "true"
    environment["AWS_CONFIG_FILE"] = str(terraform_data_dir / "no-aws-credentials")
    environment["AWS_SHARED_CREDENTIALS_FILE"] = str(
        terraform_data_dir / "no-aws-credentials"
    )
    environment["TF_DATA_DIR"] = str(terraform_data_dir)

    return subprocess.run(
        ["terraform", f"-chdir={TERRAFORM_ROOT}", *arguments],
        check=False,
        capture_output=True,
        env=environment,
        text=True,
    )


def assert_terraform_succeeded(result: subprocess.CompletedProcess[str]) -> None:
    """Fail a contract test with Terraform's full stdout and stderr."""
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.fixture(scope="module")
def initialized_terraform_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Initialize providers and modules without configuring a remote backend."""
    terraform_data_dir = tmp_path_factory.mktemp("terraform-base")
    arguments = ["init", "-backend=false", "-input=false", "-lockfile=readonly"]
    if plugin_directory := os.environ.get("TERRAFORM_PLUGIN_DIR"):
        arguments.append(f"-plugin-dir={plugin_directory}")
    assert_terraform_succeeded(run_terraform(terraform_data_dir, *arguments))
    return terraform_data_dir


def test_terraform_configuration_is_formatted_and_valid(
    initialized_terraform_root: Path,
) -> None:
    """Catch malformed or non-canonical Terraform before an operator plans it."""
    assert_terraform_succeeded(
        run_terraform(initialized_terraform_root, "fmt", "-check", "-recursive")
    )
    assert_terraform_succeeded(
        run_terraform(initialized_terraform_root, "validate", "-no-color")
    )


def test_mocked_plan_enforces_phase_1_security_and_cost_contracts(
    initialized_terraform_root: Path,
) -> None:
    """Exercise queue, database, network, and IAM plans without contacting AWS."""
    assert_terraform_succeeded(
        run_terraform(initialized_terraform_root, "test", "-no-color")
    )


def test_manifest_notification_depends_on_the_source_scoped_queue_policy(
    initialized_terraform_root: Path,
) -> None:
    """Prevent S3 notification creation from racing queue-policy propagation."""
    result = run_terraform(initialized_terraform_root, "graph", "-type=plan")
    assert_terraform_succeeded(result)
    expected_edge = (
        'module.spine.aws_s3_bucket_notification.raw_manifest_arrival (expand)" '
        '-> "[root] module.spine.aws_sqs_queue_policy.arrival (expand)'
    )
    assert expected_edge in result.stdout


def test_redrive_policies_depend_on_their_actual_queues(
    initialized_terraform_root: Path,
) -> None:
    """Prevent constructed ARNs from hiding queue replacement dependencies."""
    result = run_terraform(initialized_terraform_root, "graph", "-type=plan")
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
    root_spine_configuration = (TERRAFORM_ROOT / "spine.tf").read_text()
    assert "var.additional_loader_principals" in root_spine_configuration
    assert "var.additional_ingest_principals" not in root_spine_configuration


def test_neither_runtime_role_can_delete_raw_objects_or_read_secrets() -> None:
    """Catch privilege growth across both the ingest and loader role policies."""
    iam_configuration = "\n".join(
        (TERRAFORM_ROOT / relative_path).read_text()
        for relative_path in ("iam.tf", "modules/spine/iam.tf")
    )
    assert "s3:DeleteObject" not in iam_configuration
    assert "secretsmanager:" not in iam_configuration


def test_spine_has_no_nat_gateway() -> None:
    """Prevent a paid NAT gateway from entering the small Phase 1 network."""
    spine_configuration = "\n".join(
        path.read_text()
        for path in sorted((TERRAFORM_ROOT / "modules" / "spine").glob("*.tf"))
    )
    assert 'resource "aws_nat_gateway"' not in spine_configuration
