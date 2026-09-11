mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "123456789012"
      arn        = "arn:aws:iam::123456789012:role/TestOperator"
      id         = "123456789012"
    }
  }

  mock_data "aws_iam_role" {
    defaults = {
      arn  = "arn:aws:iam::123456789012:role/TestOperator"
      id   = "TestOperator"
      name = "TestOperator"
    }
  }
}

variables {
  environment                  = "test"
  terraform_operator_role_name = "TestOperator"
  state_key                    = "carrier-risk/base/terraform.tfstate"
}

run "rejects_state_key_in_a_different_prefix" {
  command = plan

  variables {
    state_key = "acceptance/terraform.tfstate"
  }

  expect_failures = [var.state_key]
}

run "rejects_different_state_key_in_the_same_prefix" {
  command = plan

  variables {
    state_key = "carrier-risk/acceptance.tfstate"
  }

  expect_failures = [var.state_key]
}

run "rejects_wildcard_state_key" {
  command = plan

  variables {
    state_key = "carrier-risk/*.tfstate"
  }

  expect_failures = [var.state_key]
}

run "remote_state_policy_name_is_scoped_to_test_environment" {
  command = plan

  assert {
    condition     = aws_iam_role_policy.terraform_deployment.name == "carrier-risk-remote-state-test"
    error_message = "The remote-state policy name must include the test environment."
  }
}

run "remote_state_policy_name_is_distinct_for_an_independent_environment" {
  command = plan

  variables {
    environment = "acceptance"
  }

  assert {
    condition = (
      aws_iam_role_policy.terraform_deployment.name == "carrier-risk-remote-state-acceptance" &&
      aws_iam_role_policy.terraform_deployment.name != "carrier-risk-remote-state-test"
    )
    error_message = "Independent environments must not share an inline remote-state policy name."
  }
}

run "state_bucket_and_deployment_identity_contract" {
  command = apply

  assert {
    condition = (
      !aws_s3_bucket.state.force_destroy &&
      aws_s3_bucket_versioning.state.versioning_configuration[0].status == "Enabled" &&
      one(one(aws_s3_bucket_server_side_encryption_configuration.state.rule).apply_server_side_encryption_by_default).sse_algorithm == "AES256" &&
      aws_s3_bucket_public_access_block.state.block_public_acls &&
      aws_s3_bucket_public_access_block.state.block_public_policy &&
      aws_s3_bucket_public_access_block.state.ignore_public_acls &&
      aws_s3_bucket_public_access_block.state.restrict_public_buckets
    )
    error_message = "Terraform state must be encrypted, versioned, private, and protected from accidental force deletion."
  }

  assert {
    condition = (
      jsondecode(aws_s3_bucket_policy.state.policy).Statement[0].Effect == "Deny" &&
      jsondecode(aws_s3_bucket_policy.state.policy).Statement[0].Principal == "*" &&
      toset(jsondecode(aws_s3_bucket_policy.state.policy).Statement[0].Resource) == toset([
        aws_s3_bucket.state.arn,
        "${aws_s3_bucket.state.arn}/*",
      ]) &&
      jsondecode(aws_s3_bucket_policy.state.policy).Statement[0].Condition.Bool["aws:SecureTransport"] == "false"
    )
    error_message = "The state bucket must reject every non-TLS request."
  }

  assert {
    condition = (
      aws_iam_role_policy.terraform_deployment.role == data.aws_iam_role.terraform_operator.id &&
      jsondecode(aws_iam_role_policy.terraform_deployment.policy).Statement[1].Resource == [aws_s3_bucket.state.arn] &&
      toset(jsondecode(aws_iam_role_policy.terraform_deployment.policy).Statement[1].Condition.StringEquals["s3:prefix"]) == toset([
        var.state_key,
        "${var.state_key}.tflock",
      ]) &&
      toset(jsondecode(aws_iam_role_policy.terraform_deployment.policy).Statement[2].Action) == toset(["s3:GetObject", "s3:PutObject"]) &&
      jsondecode(aws_iam_role_policy.terraform_deployment.policy).Statement[2].Resource == ["${aws_s3_bucket.state.arn}/${var.state_key}"] &&
      jsondecode(aws_iam_role_policy.terraform_deployment.policy).Statement[3].Resource == ["${aws_s3_bucket.state.arn}/${var.state_key}.tflock"] &&
      toset(jsondecode(aws_iam_role_policy.terraform_deployment.policy).Statement[3].Action) == toset(["s3:DeleteObject", "s3:GetObject", "s3:PutObject"])
    )
    error_message = "State deletion must stay denied while lock deletion is limited to the native S3 lock object."
  }
}
