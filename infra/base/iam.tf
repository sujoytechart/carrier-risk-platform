# The ingestion identity. Scoped to the raw prefix and nothing else, so a mistake
# in the download path cannot touch anything the platform did not create.

data "aws_iam_role" "terraform_operator" {
  name = var.terraform_operator_role_name
}

data "aws_iam_policy_document" "ingest_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type = "AWS"
      identifiers = distinct(concat(
        [data.aws_iam_role.terraform_operator.arn],
        var.additional_ingest_principals,
      ))
    }
  }
}

data "aws_iam_policy_document" "ingest" {
  statement {
    sid    = "ListRawBucket"
    effect = "Allow"

    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.raw.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["raw/*", "raw"]
    }
  }

  statement {
    sid    = "WriteRawObjects"
    effect = "Allow"

    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:GetObjectVersion",
    ]

    resources = ["${aws_s3_bucket.raw.arn}/raw/*"]
  }
}

resource "aws_iam_role" "ingest" {
  name               = "carrier-risk-ingest-${var.environment}"
  description        = "Writes raw FMCSA feed snapshots. No delete permission by design."
  assume_role_policy = data.aws_iam_policy_document.ingest_assume.json
}

resource "aws_iam_role_policy" "ingest" {
  name   = "raw-prefix-write"
  role   = aws_iam_role.ingest.id
  policy = data.aws_iam_policy_document.ingest.json
}

locals {
  ecr_publisher_principal_arns = distinct(concat(
    [data.aws_iam_role.terraform_operator.arn],
    var.additional_ecr_publisher_principals,
  ))
}

resource "aws_iam_role" "ecr_publisher" {
  name        = "carrier-risk-ecr-publisher-${var.environment}"
  description = "Publishes and reads images in the carrier risk API repository."
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "sts:AssumeRole"
      Principal = {
        AWS = local.ecr_publisher_principal_arns
      }
    }]
  })
}

resource "aws_iam_role_policy" "ecr_publisher" {
  name = "repository-publish"
  role = aws_iam_role.ecr_publisher.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # AWS does not support repository-level resource scoping for this API.
        Sid      = "RequestRegistryAuthorization"
        Effect   = "Allow"
        Action   = ["ecr:GetAuthorizationToken"]
        Resource = ["*"]
      },
      {
        Sid    = "PublishApiImages"
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability",
          "ecr:BatchGetImage",
          "ecr:CompleteLayerUpload",
          "ecr:GetDownloadUrlForLayer",
          "ecr:InitiateLayerUpload",
          "ecr:PutImage",
          "ecr:UploadLayerPart",
        ]
        Resource = [aws_ecr_repository.api.arn]
      },
    ]
  })
}

locals {
  analytics_principal_arns = distinct(concat(
    [data.aws_iam_role.terraform_operator.arn],
    var.additional_analytics_principals,
  ))
}

resource "aws_iam_role" "analytics" {
  name        = "carrier-risk-analytics-${var.environment}"
  description = "Publishes catalog documents and runs bounded raw snapshot queries."
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "sts:AssumeRole"
      Principal = {
        AWS = local.analytics_principal_arns
      }
    }]
  })
}

resource "aws_iam_role_policy" "analytics" {
  name = "catalog-publish-and-query"
  role = aws_iam_role.analytics.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
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
            "s3:prefix" = [
              "catalog",
              "catalog/*",
              "catalog-metadata",
              "catalog-metadata/*",
              "derived",
              "derived/*",
              "derived-metadata",
              "derived-metadata/*",
              "raw",
              "raw/*",
            ]
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
        Sid    = "ReadCatalogDocuments"
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = [
          "${aws_s3_bucket.raw.arn}/catalog/*",
          "${aws_s3_bucket.raw.arn}/catalog-metadata/*",
          "${aws_s3_bucket.raw.arn}/derived/*",
          "${aws_s3_bucket.raw.arn}/derived-metadata/*",
        ]
      },
      {
        # Catalog publication is append-only: the publisher supplies
        # If-None-Match=* and S3 rejects any attempt to replace an object.
        Sid    = "PublishCatalogDocuments"
        Effect = "Allow"
        Action = ["s3:PutObject"]
        Resource = [
          "${aws_s3_bucket.raw.arn}/catalog/*",
          "${aws_s3_bucket.raw.arn}/catalog-metadata/*",
          "${aws_s3_bucket.raw.arn}/derived/*",
          "${aws_s3_bucket.raw.arn}/derived-metadata/*",
        ]
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
        Sid    = "RunBoundedQueries"
        Effect = "Allow"
        Action = [
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StartQueryExecution",
          "athena:StopQueryExecution",
        ]
        Resource = [aws_athena_workgroup.raw_analysis.arn]
      },
      {
        Sid    = "ReadRawCatalog"
        Effect = "Allow"
        Action = [
          "glue:BatchGetPartition",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:GetTable",
          "glue:GetTables",
        ]
        Resource = [
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
          aws_glue_catalog_database.raw.arn,
          aws_glue_catalog_table.raw["crashes"].arn,
          aws_glue_catalog_table.raw["inspections"].arn,
          aws_glue_catalog_table.snapshot_metadata.arn,
          aws_glue_catalog_table.derived["crashes"].arn,
          aws_glue_catalog_table.derived["inspections"].arn,
          aws_glue_catalog_table.derived_metadata.arn,
        ]
      },
    ]
  })
}
