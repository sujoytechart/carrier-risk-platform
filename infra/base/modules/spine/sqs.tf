locals {
  arrival_queue_name     = "carrier-risk-arrival-${var.environment}"
  arrival_queue_arn      = "arn:aws:sqs:${var.region}:${var.account_id}:${local.arrival_queue_name}"
  dead_letter_queue_name = "carrier-risk-arrival-dlq-${var.environment}"
  dead_letter_queue_arn  = "arn:aws:sqs:${var.region}:${var.account_id}:${local.dead_letter_queue_name}"
}

resource "aws_sqs_queue" "dead_letter" {
  name                      = local.dead_letter_queue_name
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
  tags                      = var.common_tags
}

resource "aws_sqs_queue" "arrival" {
  name                       = local.arrival_queue_name
  receive_wait_time_seconds  = 20
  visibility_timeout_seconds = 3600
  sqs_managed_sse_enabled    = true
  tags                       = var.common_tags

  redrive_policy = jsonencode({
    deadLetterTargetArn = local.dead_letter_queue_arn
    maxReceiveCount     = 5
  })
}

resource "aws_sqs_queue_redrive_allow_policy" "dead_letter" {
  queue_url = aws_sqs_queue.dead_letter.id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [local.arrival_queue_arn]
  })
}

resource "aws_sqs_queue_policy" "arrival" {
  queue_url = aws_sqs_queue.arrival.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "AllowRawBucketManifestNotifications"
      Effect = "Allow"
      Principal = {
        Service = "s3.amazonaws.com"
      }
      Action   = "sqs:SendMessage"
      Resource = local.arrival_queue_arn
      Condition = {
        ArnEquals = {
          "aws:SourceArn" = var.raw_bucket_arn
        }
        StringEquals = {
          "aws:SourceAccount" = var.account_id
        }
      }
    }]
  })
}

resource "aws_s3_bucket_notification" "raw_manifest_arrival" {
  bucket = var.raw_bucket_id

  queue {
    queue_arn     = aws_sqs_queue.arrival.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "raw/"
    filter_suffix = "/manifest.json"
  }

  depends_on = [aws_sqs_queue_policy.arrival]
}
