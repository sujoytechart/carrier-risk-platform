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

  mock_resource "aws_athena_workgroup" {
    defaults = {
      arn = "arn:aws:athena:us-east-1:123456789012:workgroup/carrier-risk-raw-analysis-test"
    }
  }
}

variables {
  environment                  = "test"
  terraform_operator_role_name = "TestOperator"
  catalog_acquisition_dates = {
    crashes     = "2026-09-03"
    inspections = "2026-09-04"
  }
}

run "catalog_schema_and_partition_contract" {
  command = apply

  assert {
    condition = [
      for column in aws_glue_catalog_table.raw["crashes"].storage_descriptor[0].columns :
      column.name
      ] == [
      for column_name in jsondecode(file("${path.root}/../../ingest/schemas/crashes.json")).columns :
      lower(column_name)
    ]
    error_message = "raw_crashes must preserve every canonical schema column in order and lowercase it."
  }

  assert {
    condition = alltrue([
      for column in aws_glue_catalog_table.raw["crashes"].storage_descriptor[0].columns :
      column.type == "string"
    ])
    error_message = "Every raw_crashes source column must remain a string."
  }

  assert {
    condition = [
      for column in aws_glue_catalog_table.raw["inspections"].storage_descriptor[0].columns :
      column.name
      ] == [
      for column_name in jsondecode(file("${path.root}/../../ingest/schemas/inspections.json")).columns :
      lower(column_name)
    ]
    error_message = "raw_inspections must preserve every canonical schema column in order and lowercase it."
  }

  assert {
    condition = alltrue([
      for column in aws_glue_catalog_table.raw["inspections"].storage_descriptor[0].columns :
      column.type == "string"
    ])
    error_message = "Every raw_inspections source column must remain a string."
  }

  assert {
    condition = (
      aws_glue_catalog_table.raw["crashes"].name == "raw_crashes" &&
      aws_glue_catalog_table.raw["inspections"].name == "raw_inspections" &&
      aws_glue_catalog_table.raw["crashes"].storage_descriptor[0].location == "s3://${aws_s3_bucket.raw.id}/catalog/feed=crashes/" &&
      aws_glue_catalog_table.raw["inspections"].storage_descriptor[0].location == "s3://${aws_s3_bucket.raw.id}/catalog/feed=inspections/" &&
      aws_glue_partition.raw["crashes"].storage_descriptor[0].location == "s3://${aws_s3_bucket.raw.id}/catalog/feed=crashes/acquisition_date=2026-09-03/" &&
      aws_glue_partition.raw["inspections"].storage_descriptor[0].location == "s3://${aws_s3_bucket.raw.id}/catalog/feed=inspections/acquisition_date=2026-09-04/" &&
      aws_glue_partition.raw["crashes"].partition_values == tolist(["2026-09-03"]) &&
      aws_glue_partition.raw["inspections"].partition_values == tolist(["2026-09-04"])
    )
    error_message = "Raw tables and partitions must point only at their feed-specific catalog prefixes."
  }

  assert {
    condition = alltrue([
      for table in values(aws_glue_catalog_table.raw) :
      table.storage_descriptor[0].input_format == "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat" &&
      table.storage_descriptor[0].compressed &&
      table.storage_descriptor[0].ser_de_info[0].serialization_library == "org.apache.hadoop.hive.serde2.OpenCSVSerde" &&
      length(table.storage_descriptor[0].ser_de_info[0].parameters) == 3 &&
      table.storage_descriptor[0].ser_de_info[0].parameters["escapeChar"] == "\u0000" &&
      table.storage_descriptor[0].ser_de_info[0].parameters["quoteChar"] == "\"" &&
      table.storage_descriptor[0].ser_de_info[0].parameters["separatorChar"] == "," &&
      table.parameters["skip.header.line.count"] == "1" &&
      table.parameters["compressionType"] == "gzip"
    ])
    error_message = "Raw tables must use the approved symlink, CSV, header, and gzip settings."
  }

  assert {
    condition = alltrue([
      for partition in values(aws_glue_partition.raw) :
      partition.storage_descriptor[0].input_format == "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat" &&
      partition.storage_descriptor[0].compressed &&
      partition.storage_descriptor[0].ser_de_info[0].serialization_library == "org.apache.hadoop.hive.serde2.OpenCSVSerde" &&
      length(partition.storage_descriptor[0].ser_de_info[0].parameters) == 3 &&
      partition.storage_descriptor[0].ser_de_info[0].parameters["escapeChar"] == "\u0000" &&
      partition.storage_descriptor[0].ser_de_info[0].parameters["quoteChar"] == "\"" &&
      partition.storage_descriptor[0].ser_de_info[0].parameters["separatorChar"] == ","
    ])
    error_message = "Each raw partition must preserve the table's symlink, CSV, and gzip settings."
  }

  assert {
    condition = [
      for column in aws_glue_catalog_table.snapshot_metadata.storage_descriptor[0].columns :
      { name = column.name, type = column.type }
      ] == [
      { name = "batch_id", type = "string" },
      { name = "content_sha256", type = "string" },
      { name = "object_sha256", type = "string" },
      { name = "observed_at", type = "string" },
      { name = "row_count", type = "bigint" },
      { name = "schema_fingerprint", type = "string" },
    ]
    error_message = "Metadata columns must preserve the manifest contract without duplicating partition feed."
  }

  assert {
    condition = (
      length(aws_glue_catalog_table.snapshot_metadata.partition_keys) == 2 &&
      aws_glue_catalog_table.snapshot_metadata.partition_keys[0].name == "feed" &&
      aws_glue_catalog_table.snapshot_metadata.partition_keys[0].type == "string" &&
      aws_glue_catalog_table.snapshot_metadata.partition_keys[1].name == "acquisition_date" &&
      aws_glue_catalog_table.snapshot_metadata.partition_keys[1].type == "string" &&
      aws_glue_catalog_table.snapshot_metadata.storage_descriptor[0].ser_de_info[0].serialization_library == "org.openx.data.jsonserde.JsonSerDe" &&
      aws_glue_catalog_table.snapshot_metadata.storage_descriptor[0].location == "s3://${aws_s3_bucket.raw.id}/catalog-metadata/" &&
      aws_glue_partition.snapshot_metadata["crashes"].storage_descriptor[0].location == "s3://${aws_s3_bucket.raw.id}/catalog-metadata/feed=crashes/acquisition_date=2026-09-03/" &&
      aws_glue_partition.snapshot_metadata["inspections"].storage_descriptor[0].location == "s3://${aws_s3_bucket.raw.id}/catalog-metadata/feed=inspections/acquisition_date=2026-09-04/" &&
      aws_glue_partition.snapshot_metadata["crashes"].partition_values == tolist(["crashes", "2026-09-03"]) &&
      aws_glue_partition.snapshot_metadata["inspections"].partition_values == tolist(["inspections", "2026-09-04"])
    )
    error_message = "Metadata partitions must use the exact feed and acquisition-date paths."
  }
}

run "athena_results_and_workgroup_contract" {
  command = apply

  assert {
    condition = (
      aws_s3_bucket.athena_results.force_destroy &&
      aws_s3_bucket.athena_results.id != aws_s3_bucket.raw.id &&
      aws_s3_bucket_ownership_controls.athena_results.rule[0].object_ownership == "BucketOwnerEnforced" &&
      aws_s3_bucket_public_access_block.athena_results.block_public_acls &&
      aws_s3_bucket_public_access_block.athena_results.block_public_policy &&
      aws_s3_bucket_public_access_block.athena_results.ignore_public_acls &&
      aws_s3_bucket_public_access_block.athena_results.restrict_public_buckets &&
      one(aws_s3_bucket_server_side_encryption_configuration.athena_results.rule).apply_server_side_encryption_by_default[0].sse_algorithm == "AES256" &&
      one([for rule in aws_s3_bucket_lifecycle_configuration.athena_results.rule : rule if rule.id == "expire-query-results"]).status == "Enabled" &&
      one([for rule in aws_s3_bucket_lifecycle_configuration.athena_results.rule : rule if rule.id == "expire-query-results"]).filter[0].prefix == "queries/" &&
      one([for rule in aws_s3_bucket_lifecycle_configuration.athena_results.rule : rule if rule.id == "expire-query-results"]).expiration[0].days == var.athena_results_retention_days &&
      one([for rule in aws_s3_bucket_lifecycle_configuration.athena_results.rule : rule if rule.id == "abort-incomplete-multipart-uploads"]).status == "Enabled" &&
      one([for rule in aws_s3_bucket_lifecycle_configuration.athena_results.rule : rule if rule.id == "abort-incomplete-multipart-uploads"]).abort_incomplete_multipart_upload[0].days_after_initiation == 1
    )
    error_message = "Athena results must use a separate private encrypted bucket with bounded cleanup."
  }

  assert {
    condition = jsondecode(aws_s3_bucket_policy.athena_results_transport.policy).Statement == [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Action    = "s3:*"
      Principal = "*"
      Resource  = [aws_s3_bucket.athena_results.arn, "${aws_s3_bucket.athena_results.arn}/*"]
      Condition = {
        Bool = {
          "aws:SecureTransport" = "false"
        }
      }
    }]
    error_message = "The query-results bucket must reject requests that do not use TLS."
  }

  assert {
    condition = (
      aws_athena_workgroup.raw_analysis.configuration[0].enforce_workgroup_configuration &&
      aws_athena_workgroup.raw_analysis.configuration[0].publish_cloudwatch_metrics_enabled &&
      aws_athena_workgroup.raw_analysis.configuration[0].bytes_scanned_cutoff_per_query == var.athena_query_scan_cutoff_bytes &&
      aws_athena_workgroup.raw_analysis.configuration[0].result_configuration[0].output_location == "s3://${aws_s3_bucket.athena_results.id}/queries/" &&
      aws_athena_workgroup.raw_analysis.configuration[0].result_configuration[0].expected_bucket_owner == data.aws_caller_identity.current.account_id &&
      aws_athena_workgroup.raw_analysis.configuration[0].result_configuration[0].encryption_configuration[0].encryption_option == "SSE_S3"
    )
    error_message = "The workgroup must enforce output ownership, encryption, metrics, and the scan cutoff."
  }
}

run "analytics_role_least_privilege_contract" {
  command = apply

  assert {
    condition = jsondecode(aws_iam_role.analytics.assume_role_policy).Statement == [{
      Effect = "Allow"
      Action = "sts:AssumeRole"
      Principal = {
        AWS = [data.aws_iam_role.terraform_operator.arn]
      }
    }]
    error_message = "Analytics trust must use its dedicated principal set."
  }

  assert {
    condition = jsondecode(aws_iam_role_policy.analytics.policy).Statement == [
      {
        Sid      = "GetRawBucketLocation"
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation"]
        Resource = [aws_s3_bucket.raw.arn]
      },
      {
        Sid      = "ListRawAndCatalogPrefixes"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [aws_s3_bucket.raw.arn]
        Condition = {
          StringLike = {
            "s3:prefix" = ["catalog", "catalog/*", "catalog-metadata", "catalog-metadata/*", "raw", "raw/*"]
          }
        }
      },
      {
        Sid      = "ReadRawSnapshotEvidence"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = ["${aws_s3_bucket.raw.arn}/raw/*"]
      },
      {
        Sid      = "ReadCatalogDocuments"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${aws_s3_bucket.raw.arn}/catalog/*", "${aws_s3_bucket.raw.arn}/catalog-metadata/*"]
      },
      {
        Sid      = "PublishCatalogDocuments"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = ["${aws_s3_bucket.raw.arn}/catalog/*", "${aws_s3_bucket.raw.arn}/catalog-metadata/*"]
        Condition = {
          StringEquals = {
            "s3:if-none-match" = "*"
          }
        }
      },
      {
        Sid      = "GetResultsBucketLocation"
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation"]
        Resource = [aws_s3_bucket.athena_results.arn]
      },
      {
        Sid      = "ListQueryResults"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [aws_s3_bucket.athena_results.arn]
        Condition = {
          StringLike = {
            "s3:prefix" = ["queries", "queries/*"]
          }
        }
      },
      {
        Sid      = "ReadWriteQueryResults"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = ["${aws_s3_bucket.athena_results.arn}/queries/*"]
      },
      {
        Sid      = "RunBoundedQueries"
        Effect   = "Allow"
        Action   = ["athena:GetQueryExecution", "athena:GetQueryResults", "athena:StartQueryExecution", "athena:StopQueryExecution"]
        Resource = [aws_athena_workgroup.raw_analysis.arn]
      },
      {
        Sid    = "ReadRawCatalog"
        Effect = "Allow"
        Action = ["glue:BatchGetPartition", "glue:GetDatabase", "glue:GetDatabases", "glue:GetPartition", "glue:GetPartitions", "glue:GetTable", "glue:GetTables"]
        Resource = [
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
          aws_glue_catalog_database.raw.arn,
          aws_glue_catalog_table.raw["crashes"].arn,
          aws_glue_catalog_table.raw["inspections"].arn,
          aws_glue_catalog_table.snapshot_metadata.arn,
        ]
      },
    ]
    error_message = "Analytics policy must contain only the exact read, catalog-publish, query, and result privileges."
  }
}

run "partitions_are_opt_in_by_default" {
  command = plan

  variables {
    catalog_acquisition_dates = {}
  }

  assert {
    condition = (
      length(aws_glue_partition.raw) == 0 &&
      length(aws_glue_partition.snapshot_metadata) == 0
    )
    error_message = "Terraform must create no catalog partitions until both feed dates are selected."
  }
}

run "reject_partial_partition_selection" {
  command = plan

  variables {
    catalog_acquisition_dates = { crashes = "2026-09-03" }
  }

  expect_failures = [var.catalog_acquisition_dates]
}

run "reject_invalid_partition_date" {
  command = plan

  variables {
    catalog_acquisition_dates = {
      crashes     = "2026-02-30"
      inspections = "2026-09-04"
    }
  }

  expect_failures = [var.catalog_acquisition_dates]
}

run "reject_year_zero_partition_date" {
  command = plan

  variables {
    catalog_acquisition_dates = {
      crashes     = "0000-01-01"
      inspections = "2026-09-04"
    }
  }

  expect_failures = [var.catalog_acquisition_dates]
}

run "reject_unsupported_partition_feed" {
  command = plan

  variables {
    catalog_acquisition_dates = {
      crashes     = "2026-09-03"
      inspections = "2026-09-04"
      vehicles    = "2026-09-05"
    }
  }

  expect_failures = [var.catalog_acquisition_dates]
}

run "reject_results_retention_below_minimum" {
  command = plan

  variables {
    athena_results_retention_days = 0
  }

  expect_failures = [var.athena_results_retention_days]
}

run "reject_results_retention_above_maximum" {
  command = plan

  variables {
    athena_results_retention_days = 31
  }

  expect_failures = [var.athena_results_retention_days]
}

run "reject_fractional_results_retention" {
  command = plan

  variables {
    athena_results_retention_days = 7.5
  }

  expect_failures = [var.athena_results_retention_days]
}

run "reject_scan_cutoff_below_minimum" {
  command = plan

  variables {
    athena_query_scan_cutoff_bytes = 10485759
  }

  expect_failures = [var.athena_query_scan_cutoff_bytes]
}

run "reject_scan_cutoff_above_maximum" {
  command = plan

  variables {
    athena_query_scan_cutoff_bytes = 10737418241
  }

  expect_failures = [var.athena_query_scan_cutoff_bytes]
}

run "reject_fractional_scan_cutoff" {
  command = plan

  variables {
    athena_query_scan_cutoff_bytes = 1073741824.5
  }

  expect_failures = [var.athena_query_scan_cutoff_bytes]
}
