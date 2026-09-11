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

  mock_resource "aws_ecr_repository" {
    defaults = {
      arn            = "arn:aws:ecr:us-east-1:123456789012:repository/carrier-risk-api-test"
      registry_id    = "123456789012"
      repository_url = "123456789012.dkr.ecr.us-east-1.amazonaws.com/carrier-risk-api-test"
    }
  }
}

variables {
  environment                         = "test"
  terraform_operator_role_name        = "TestOperator"
  additional_ecr_publisher_principals = []
  ecr_retained_image_count            = 10
}

run "ecr_security_and_retention_contract" {
  command = apply

  assert {
    condition = (
      aws_ecr_repository.api.image_tag_mutability == "IMMUTABLE" &&
      aws_ecr_repository.api.image_scanning_configuration[0].scan_on_push &&
      aws_ecr_repository.api.encryption_configuration[0].encryption_type == "AES256" &&
      aws_ecr_repository.api.force_delete
    )
    error_message = "The API registry must be encrypted, scan immutable tags, and remain destroyable after test pushes."
  }

  assert {
    condition = (
      jsondecode(aws_ecr_lifecycle_policy.api.policy).rules[0].selection.countType == "imageCountMoreThan" &&
      jsondecode(aws_ecr_lifecycle_policy.api.policy).rules[0].selection.tagStatus == "any" &&
      jsondecode(aws_ecr_lifecycle_policy.api.policy).rules[0].selection.countNumber == var.ecr_retained_image_count &&
      jsondecode(aws_ecr_lifecycle_policy.api.policy).rules[0].action.type == "expire"
    )
    error_message = "The registry must expire images beyond the configured retention count."
  }

  assert {
    condition = (
      jsondecode(aws_iam_role.ecr_publisher.assume_role_policy).Statement[0].Principal.AWS == [data.aws_iam_role.terraform_operator.arn] &&
      jsondecode(aws_iam_role_policy.ecr_publisher.policy).Statement[0].Action == ["ecr:GetAuthorizationToken"] &&
      jsondecode(aws_iam_role_policy.ecr_publisher.policy).Statement[0].Resource == ["*"] &&
      jsondecode(aws_iam_role_policy.ecr_publisher.policy).Statement[1].Resource == [aws_ecr_repository.api.arn] &&
      toset(jsondecode(aws_iam_role_policy.ecr_publisher.policy).Statement[1].Action) == toset([
        "ecr:BatchCheckLayerAvailability",
        "ecr:BatchGetImage",
        "ecr:CompleteLayerUpload",
        "ecr:GetDownloadUrlForLayer",
        "ecr:InitiateLayerUpload",
        "ecr:PutImage",
        "ecr:UploadLayerPart",
      ])
    )
    error_message = "Publisher access must use the required wildcard only for authorization and scope image operations to one repository."
  }
}
