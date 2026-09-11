output "state_bucket_name" {
  description = "Bucket name to place in the base root's ignored backend.hcl."
  value       = aws_s3_bucket.state.id
  sensitive   = true
}

output "state_key" {
  description = "State key fixed in both the bootstrap policy and base backend."
  value       = var.state_key
}

output "terraform_deployment_role_arn" {
  description = "Role receiving state access, for backend assume_role when separate from the deployment operator."
  value       = data.aws_iam_role.terraform_operator.arn
  sensitive   = true
}
