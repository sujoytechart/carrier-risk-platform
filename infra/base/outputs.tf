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

output "api_repository_url" {
  description = "URL of the ECR repository that stores versioned API images."
  value       = aws_ecr_repository.api.repository_url
  sensitive   = true
}

output "ecr_publisher_role_arn" {
  description = "Role allowed to publish images to the API repository."
  value       = aws_iam_role.ecr_publisher.arn
  sensitive   = true
}

output "arrival_queue_arn" {
  description = "ARN of the manifest arrival queue, or null when the spine is disabled."
  value       = try(module.spine[0].arrival_queue_arn, null)
}

output "arrival_queue_url" {
  description = "URL of the manifest arrival queue, or null when the spine is disabled."
  value       = try(module.spine[0].arrival_queue_url, null)
}

output "dead_letter_queue_arn" {
  description = "ARN of the arrival DLQ, or null when the spine is disabled."
  value       = try(module.spine[0].dead_letter_queue_arn, null)
}

output "loader_role_arn" {
  description = "Loader role ARN, or null when the spine is disabled."
  value       = try(module.spine[0].loader_role_arn, null)
}

output "warehouse_connection" {
  description = "Password-free RDS connection metadata, or null when disabled."
  sensitive   = true
  value       = try(module.spine[0].warehouse_connection, null)
}

output "warehouse_master_secret_arn" {
  description = "RDS-managed credential secret ARN, or null when disabled."
  sensitive   = true
  value       = try(module.spine[0].warehouse_master_secret_arn, null)
}
