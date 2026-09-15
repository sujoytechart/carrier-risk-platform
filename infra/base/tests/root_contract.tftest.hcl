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
      name = "TestOperator"
    }
  }

  # Root policy-document data sources are provider-local helpers. Supply valid
  # JSON so this test can focus on the spine gate rather than their internals.
  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }

  mock_data "aws_availability_zones" {
    defaults = {
      names = ["us-east-1a", "us-east-1b"]
    }
  }
}

run "spine_is_disabled_by_default" {
  command = plan

  assert {
    condition     = length(module.spine) == 0
    error_message = "The cost-bearing Phase 1 spine must be disabled by default."
  }

  assert {
    condition = (
      output.arrival_queue_arn == null &&
      output.warehouse_connection == null &&
      output.warehouse_master_secret_arn == null
    )
    error_message = "Disabled spine outputs must not imply that runtime resources exist."
  }
}
