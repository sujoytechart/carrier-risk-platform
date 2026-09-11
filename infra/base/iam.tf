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
