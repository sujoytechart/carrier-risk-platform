variable "region" {
  description = "AWS region for the state bucket and operator policy."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name used in resource names and tags."
  type        = string
  default     = "dev"
}

variable "state_bucket_prefix" {
  description = "Globally unique state bucket prefix; the AWS account id is appended."
  type        = string
  default     = "carrier-risk-tf-state"
}

variable "state_key" {
  description = "Exact object key used by the base root and its native S3 lockfile."
  type        = string
  default     = "carrier-risk/base/terraform.tfstate"

  validation {
    condition     = var.state_key == "carrier-risk/base/terraform.tfstate"
    error_message = "state_key must exactly match the fixed base backend key: carrier-risk/base/terraform.tfstate."
  }
}

variable "terraform_operator_role_name" {
  description = "Stable IAM role that deploys the base root and accesses its state."
  type        = string
}
