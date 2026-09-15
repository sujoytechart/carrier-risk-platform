locals {
  athena_results_bucket_name = "${var.athena_results_bucket_prefix}-${data.aws_caller_identity.current.account_id}"
}

# Query results are disposable derived data. force_destroy lets an approved
# stack teardown remove remaining results; raw evidence keeps its existing policy.
resource "aws_s3_bucket" "athena_results" {
  bucket        = local.athena_results_bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_ownership_controls" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "expire-query-results"
    status = "Enabled"

    filter {
      prefix = "queries/"
    }

    expiration {
      days = var.athena_results_retention_days
    }
  }

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_s3_bucket_policy" "athena_results_transport" {
  bucket = aws_s3_bucket.athena_results.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Action    = "s3:*"
      Principal = "*"
      Resource = [
        aws_s3_bucket.athena_results.arn,
        "${aws_s3_bucket.athena_results.arn}/*",
      ]
      Condition = {
        Bool = {
          "aws:SecureTransport" = "false"
        }
      }
    }]
  })
}

# The cutoff bounds each query, but Athena may scan beyond it before cancellation.
# It is a guardrail for individual queries, not an account-level spending limit.
resource "aws_athena_workgroup" "raw_analysis" {
  name          = "carrier-risk-raw-analysis-${var.environment}"
  description   = "Queries explicit raw snapshot partitions."
  force_destroy = true
  state         = "ENABLED"

  configuration {
    bytes_scanned_cutoff_per_query     = var.athena_query_scan_cutoff_bytes
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    requester_pays_enabled             = false

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      expected_bucket_owner = data.aws_caller_identity.current.account_id
      output_location       = "s3://${aws_s3_bucket.athena_results.id}/queries/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
