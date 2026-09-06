variable "account_id" {
  description = "AWS account that owns the raw bucket and arrival queue."
  type        = string
}

variable "common_tags" {
  description = "Ownership tags applied to every taggable spine resource."
  type        = map(string)
}

variable "developer_ipv4_cidr" {
  description = <<-EOT
    The one developer host allowed to reach PostgreSQL when public access is
    explicitly enabled. Private mode requires null; public mode requires one
    valid IPv4 /32 CIDR.
  EOT
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = var.publicly_accessible ? (
      var.developer_ipv4_cidr != null &&
      can(cidrnetmask(var.developer_ipv4_cidr)) &&
      length(split(".", split("/", var.developer_ipv4_cidr)[0])) == 4 &&
      endswith(var.developer_ipv4_cidr, "/32")
    ) : var.developer_ipv4_cidr == null
    error_message = "developer_ipv4_cidr must be null in private mode or one valid IPv4 /32 CIDR in public mode."
  }
}

variable "environment" {
  description = "Environment suffix used in deterministic resource names."
  type        = string
}

variable "loader_principal_arns" {
  description = "Stable IAM principals allowed to assume the event loader role."
  type        = list(string)
}

variable "publicly_accessible" {
  description = "Whether RDS receives a public endpoint and one-host ingress."
  type        = bool
  default     = false
}

variable "raw_bucket_arn" {
  description = "ARN of the existing Phase 0 raw snapshot bucket."
  type        = string
}

variable "raw_bucket_id" {
  description = "Name of the existing Phase 0 raw snapshot bucket."
  type        = string
}
