output "raw_bucket_name" {
  description = "Bucket holding immutable raw feed snapshots."
  value       = aws_s3_bucket.raw.id
}

output "raw_bucket_arn" {
  value = aws_s3_bucket.raw.arn
}

output "ingest_role_arn" {
  description = "Role the ingestion job assumes to write raw snapshots."
  value       = aws_iam_role.ingest.arn
}
