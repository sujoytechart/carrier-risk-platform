data "aws_iam_role" "terraform_operator" {
  name = var.terraform_operator_role_name
}

locals {
  state_object_arn = "${aws_s3_bucket.state.arn}/${var.state_key}"
  lock_object_arn  = "${local.state_object_arn}.tflock"
}

resource "aws_iam_role_policy" "terraform_deployment" {
  name = "remote-state-access"
  role = data.aws_iam_role.terraform_operator.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadStateBucketLocation"
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation"]
        Resource = [aws_s3_bucket.state.arn]
      },
      {
        Sid      = "ListStateAndLock"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [aws_s3_bucket.state.arn]
        Condition = {
          StringEquals = {
            "s3:prefix" = [var.state_key, "${var.state_key}.tflock"]
          }
        }
      },
      {
        Sid      = "ReadWriteState"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = [local.state_object_arn]
      },
      {
        Sid      = "ReadWriteDeleteLock"
        Effect   = "Allow"
        Action   = ["s3:DeleteObject", "s3:GetObject", "s3:PutObject"]
        Resource = [local.lock_object_arn]
      },
    ]
  })
}
