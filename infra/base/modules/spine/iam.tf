resource "aws_iam_role" "loader" {
  name        = "carrier-risk-loader-${var.environment}"
  description = "Reads immutable snapshots and consumes their arrival messages."
  tags        = var.common_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        AWS = var.loader_principal_arns
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "loader" {
  name = "raw-read-and-arrival-consume"
  role = aws_iam_role.loader.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListRawPrefix"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [var.raw_bucket_arn]
        Condition = {
          StringLike = {
            "s3:prefix" = ["raw", "raw/*"]
          }
        }
      },
      {
        Sid      = "ReadRawObjects"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = ["${var.raw_bucket_arn}/raw/*"]
      },
      {
        Sid    = "ConsumeArrivalMessages"
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:ChangeMessageVisibility",
          "sqs:GetQueueAttributes",
          "sqs:GetQueueUrl",
        ]
        Resource = [aws_sqs_queue.arrival.arn]
      },
    ]
  })
}
