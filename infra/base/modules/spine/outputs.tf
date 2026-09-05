output "arrival_queue_arn" {
  description = "ARN of the manifest arrival queue."
  value       = aws_sqs_queue.arrival.arn
}

output "arrival_queue_url" {
  description = "URL of the manifest arrival queue."
  value       = aws_sqs_queue.arrival.url
}

output "dead_letter_queue_arn" {
  description = "ARN of the failed-message dead-letter queue."
  value       = aws_sqs_queue.dead_letter.arn
}

output "loader_role_arn" {
  description = "Role assumed by the event loader."
  value       = aws_iam_role.loader.arn
}

output "warehouse_connection" {
  description = "Password-free warehouse connection metadata."
  sensitive   = true
  value = {
    database = aws_db_instance.warehouse.db_name
    endpoint = aws_db_instance.warehouse.address
    port     = aws_db_instance.warehouse.port
    username = aws_db_instance.warehouse.username
  }
}

output "warehouse_master_secret_arn" {
  description = "ARN of the RDS-managed master credential secret."
  sensitive   = true
  value       = aws_db_instance.warehouse.master_user_secret[0].secret_arn
}
