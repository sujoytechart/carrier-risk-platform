mock_provider "aws" {
  mock_data "aws_availability_zones" {
    defaults = {
      names = ["us-east-1a", "us-east-1b"]
    }
  }

  mock_resource "aws_sqs_queue" {
    defaults = {
      arn = "arn:aws:sqs:us-east-1:123456789012:mocked-queue"
      url = "https://sqs.us-east-1.amazonaws.com/123456789012/mocked-queue"
    }
  }

  mock_resource "aws_db_instance" {
    defaults = {
      address = "mocked-warehouse.example.test"
      port    = 5432
      master_user_secret = [{
        kms_key_id    = null
        secret_arn    = "arn:aws:secretsmanager:us-east-1:123456789012:secret:mocked"
        secret_status = "active"
      }]
    }
  }
}

variables {
  raw_bucket_id         = "carrier-risk-raw-123456789012"
  raw_bucket_arn        = "arn:aws:s3:::carrier-risk-raw-123456789012"
  account_id            = "123456789012"
  environment           = "test"
  loader_principal_arns = ["arn:aws:iam::123456789012:role/TestOperator"]
  publicly_accessible   = false
  developer_ipv4_cidr   = null
  common_tags = {
    Project     = "carrier-risk-platform"
    ManagedBy   = "terraform"
    Environment = "test"
  }
}

run "private_spine_contract" {
  command = apply

  module {
    source = "./modules/spine"
  }

  assert {
    condition = (
      aws_sqs_queue.arrival.sqs_managed_sse_enabled &&
      aws_sqs_queue.dead_letter.sqs_managed_sse_enabled
    )
    error_message = "The arrival queue and DLQ must use SQS-managed encryption."
  }

  assert {
    condition = (
      aws_sqs_queue.arrival.receive_wait_time_seconds == 20 &&
      aws_sqs_queue.arrival.visibility_timeout_seconds == 3600 &&
      jsondecode(aws_sqs_queue_redrive_policy.arrival.redrive_policy).maxReceiveCount == 5 &&
      jsondecode(aws_sqs_queue_redrive_policy.arrival.redrive_policy).deadLetterTargetArn == aws_sqs_queue.dead_letter.arn &&
      jsondecode(aws_sqs_queue_redrive_allow_policy.dead_letter.redrive_allow_policy).redrivePermission == "byQueue" &&
      jsondecode(aws_sqs_queue_redrive_allow_policy.dead_letter.redrive_allow_policy).sourceQueueArns == [aws_sqs_queue.arrival.arn]
    )
    error_message = "The loader queue must use long polling, a one-hour visibility timeout, and five-receive redrive."
  }

  assert {
    condition = (
      jsondecode(aws_sqs_queue_policy.arrival.policy).Statement[0].Principal.Service == "s3.amazonaws.com" &&
      jsondecode(aws_sqs_queue_policy.arrival.policy).Statement[0].Action == "sqs:SendMessage" &&
      jsondecode(aws_sqs_queue_policy.arrival.policy).Statement[0].Resource == aws_sqs_queue.arrival.arn &&
      jsondecode(aws_sqs_queue_policy.arrival.policy).Statement[0].Condition.ArnEquals["aws:SourceArn"] == var.raw_bucket_arn &&
      jsondecode(aws_sqs_queue_policy.arrival.policy).Statement[0].Condition.StringEquals["aws:SourceAccount"] == var.account_id
    )
    error_message = "Only the account's raw bucket may publish to the arrival queue."
  }

  assert {
    condition = (
      aws_s3_bucket_notification.raw_manifest_arrival.queue[0].filter_prefix == "raw/" &&
      aws_s3_bucket_notification.raw_manifest_arrival.queue[0].filter_suffix == "/manifest.json" &&
      contains(aws_s3_bucket_notification.raw_manifest_arrival.queue[0].events, "s3:ObjectCreated:*")
    )
    error_message = "S3 notifications must select only raw manifest object-creation events."
  }

  assert {
    condition = (
      aws_db_instance.warehouse.engine == "postgres" &&
      aws_db_instance.warehouse.engine_version == "17" &&
      aws_db_instance.warehouse.instance_class == "db.t4g.micro" &&
      aws_db_instance.warehouse.allocated_storage == 20 &&
      aws_db_instance.warehouse.max_allocated_storage == null &&
      aws_db_instance.warehouse.storage_type == "gp3" &&
      aws_db_instance.warehouse.storage_encrypted &&
      !aws_db_instance.warehouse.multi_az &&
      aws_db_instance.warehouse.username == "carrier_admin" &&
      length(aws_db_instance.warehouse.username) <= 16
    )
    error_message = "The warehouse must remain the encrypted, single-AZ, cost-bounded PostgreSQL 17 instance."
  }

  assert {
    condition = (
      !aws_db_instance.warehouse.publicly_accessible &&
      length(aws_vpc_security_group_ingress_rule.developer) == 0 &&
      length(aws_internet_gateway.public) == 0
    )
    error_message = "Private mode must expose no developer ingress or internet route."
  }

  assert {
    condition = (
      aws_db_instance.warehouse.manage_master_user_password &&
      aws_db_instance.warehouse.password == null &&
      !aws_db_instance.warehouse.deletion_protection &&
      aws_db_instance.warehouse.skip_final_snapshot &&
      !aws_db_instance.warehouse.performance_insights_enabled &&
      aws_db_instance.warehouse.monitoring_interval == 0
    )
    error_message = "RDS must use a managed secret and disposable, no-premium-monitoring settings."
  }

  assert {
    condition = (
      aws_db_instance.warehouse.tags.Project == "carrier-risk-platform" &&
      aws_db_instance.warehouse.tags.ManagedBy == "terraform" &&
      aws_db_instance.warehouse.tags.Environment == "test" &&
      aws_sqs_queue.arrival.tags == var.common_tags &&
      aws_sqs_queue.dead_letter.tags == var.common_tags &&
      aws_iam_role.loader.tags == var.common_tags &&
      aws_vpc.warehouse.tags.Project == var.common_tags.Project &&
      aws_vpc.warehouse.tags.ManagedBy == var.common_tags.ManagedBy &&
      aws_vpc.warehouse.tags.Environment == var.common_tags.Environment &&
      alltrue([for subnet in aws_subnet.warehouse : (
        subnet.tags.Project == var.common_tags.Project &&
        subnet.tags.ManagedBy == var.common_tags.ManagedBy &&
        subnet.tags.Environment == var.common_tags.Environment
      )]) &&
      aws_security_group.warehouse.tags == var.common_tags &&
      aws_db_subnet_group.warehouse.tags == var.common_tags
    )
    error_message = "Spine resources must carry the common ownership tags."
  }

  assert {
    condition = (
      jsondecode(aws_iam_role.loader.assume_role_policy).Statement[0].Action == "sts:AssumeRole" &&
      toset(jsondecode(aws_iam_role.loader.assume_role_policy).Statement[0].Principal.AWS) == toset(var.loader_principal_arns) &&
      jsondecode(aws_iam_role_policy.loader.policy).Statement[0].Action == ["s3:ListBucket"] &&
      jsondecode(aws_iam_role_policy.loader.policy).Statement[0].Resource == [var.raw_bucket_arn] &&
      toset(jsondecode(aws_iam_role_policy.loader.policy).Statement[1].Action) == toset(["s3:GetObject", "s3:GetObjectVersion"]) &&
      jsondecode(aws_iam_role_policy.loader.policy).Statement[1].Resource == ["${var.raw_bucket_arn}/raw/*"] &&
      toset(jsondecode(aws_iam_role_policy.loader.policy).Statement[2].Action) == toset([
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:ChangeMessageVisibility",
        "sqs:GetQueueAttributes",
        "sqs:GetQueueUrl",
      ]) &&
      jsondecode(aws_iam_role_policy.loader.policy).Statement[2].Resource == [aws_sqs_queue.arrival.arn] &&
      !strcontains(aws_iam_role_policy.loader.policy, "s3:DeleteObject") &&
      !strcontains(aws_iam_role_policy.loader.policy, "secretsmanager:")
    )
    error_message = "The loader must have exact read/consume permissions without raw deletion or secret access."
  }
}

run "public_spine_restricts_postgres_to_one_host" {
  command = plan

  module {
    source = "./modules/spine"
  }

  variables {
    publicly_accessible = true
    developer_ipv4_cidr = "203.0.113.7/32"
  }

  assert {
    condition = (
      length(aws_vpc_security_group_ingress_rule.developer) == 1 &&
      aws_vpc_security_group_ingress_rule.developer[0].cidr_ipv4 == "203.0.113.7/32" &&
      aws_vpc_security_group_ingress_rule.developer[0].from_port == 5432 &&
      aws_vpc_security_group_ingress_rule.developer[0].to_port == 5432 &&
      length(aws_internet_gateway.public) == 1 &&
      aws_internet_gateway.public[0].tags == var.common_tags &&
      aws_route_table.public[0].tags == var.common_tags &&
      aws_vpc_security_group_ingress_rule.developer[0].tags == var.common_tags
    )
    error_message = "Public mode must expose only PostgreSQL to the one reviewed developer host."
  }
}

run "public_spine_rejects_broad_cidr" {
  command = plan

  module {
    source = "./modules/spine"
  }

  variables {
    publicly_accessible = true
    developer_ipv4_cidr = "203.0.113.0/24"
  }

  expect_failures = [var.developer_ipv4_cidr]
}

run "public_spine_rejects_missing_cidr" {
  command = plan

  module {
    source = "./modules/spine"
  }

  variables {
    publicly_accessible = true
    developer_ipv4_cidr = null
  }

  expect_failures = [var.developer_ipv4_cidr]
}

run "private_spine_rejects_unused_cidr" {
  command = plan

  module {
    source = "./modules/spine"
  }

  variables {
    publicly_accessible = false
    developer_ipv4_cidr = "203.0.113.7/32"
  }

  expect_failures = [var.developer_ipv4_cidr]
}
