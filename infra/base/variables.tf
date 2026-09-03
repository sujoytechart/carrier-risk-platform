variable "region" {
  description = "AWS region. us-east-1 unless there is a reason otherwise."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name, used in tags and resource names."
  type        = string
  default     = "dev"
}

variable "terraform_operator_role_name" {
  description = <<-EOT
    Stable IAM role allowed to assume the ingestion role. Use the role name,
    not the temporary STS session ARN returned by aws sts get-caller-identity.
  EOT
  type        = string
  default     = "AccountFullAccessRole"
}

variable "additional_ingest_principals" {
  description = <<-EOT
    Extra IAM user or role ARNs allowed to assume the ingestion role, so the
    documented local command runs under the credentials the setup produces.
    Stable ARNs only, never a temporary STS session ARN.
  EOT
  type        = list(string)
  default     = []
}

variable "bucket_prefix" {
  description = <<-EOT
    Prefix for the raw data bucket. The account id is appended, because S3 bucket
    names are globally unique across all AWS accounts.
  EOT
  type        = string
  default     = "carrier-risk-raw"
}

variable "raw_retention_days" {
  description = <<-EOT
    Days before a noncurrent raw object version expires. Raw snapshots are the
    evidence base for every rebuild, so this is deliberately long.
  EOT
  type        = number
  default     = 365
}
