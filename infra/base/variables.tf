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

variable "additional_loader_principals" {
  description = <<-EOT
    Extra stable IAM user or role ARNs allowed to assume the event loader role.
    Loader access is separate from the principals allowed to write snapshots.
  EOT
  type        = list(string)
  default     = []
}

variable "additional_ecr_publisher_principals" {
  description = <<-EOT
    Extra stable IAM user or role ARNs allowed to assume the container publisher
    role. This is independent of runtime and raw-data identities.
  EOT
  type        = list(string)
  default     = []
}

variable "additional_analytics_principals" {
  description = <<-EOT
    Extra stable IAM user or role ARNs allowed to assume the catalog publisher
    and query role. This is independent of every other runtime identity.
  EOT
  type        = list(string)
  default     = []
}

variable "catalog_acquisition_dates" {
  description = <<-EOT
    One complete initial snapshot date for each feed. Leave empty until both
    dates have been validated; partial or multiple-date selection is rejected.
  EOT
  type        = map(string)
  default     = {}

  validation {
    condition = (
      length(var.catalog_acquisition_dates) == 0 ||
      toset(keys(var.catalog_acquisition_dates)) == toset(["crashes", "inspections"])
    )
    error_message = "catalog_acquisition_dates must be empty or contain exactly crashes and inspections."
  }

  validation {
    condition = alltrue([
      for acquisition_date in values(var.catalog_acquisition_dates) :
      can(regex("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", acquisition_date)) &&
      try(tonumber(substr(acquisition_date, 0, 4)), 0) >= 1 &&
      try(tonumber(substr(acquisition_date, 0, 4)), 0) <= 9999 &&
      can(formatdate("YYYY-MM-DD", "${acquisition_date}T00:00:00Z")) &&
      try(formatdate("YYYY-MM-DD", "${acquisition_date}T00:00:00Z"), "") == acquisition_date
    ])
    error_message = "Every catalog acquisition date must be a real ISO YYYY-MM-DD date."
  }
}

variable "athena_results_bucket_prefix" {
  description = "Prefix for the separate Athena query-results bucket."
  type        = string
  default     = "carrier-risk-athena-results"
}

variable "athena_results_retention_days" {
  description = "Days before disposable Athena query results expire."
  type        = number
  default     = 7

  validation {
    condition = (
      var.athena_results_retention_days >= 1 &&
      var.athena_results_retention_days <= 30 &&
      floor(var.athena_results_retention_days) == var.athena_results_retention_days
    )
    error_message = "athena_results_retention_days must be a whole number from 1 through 30."
  }
}

variable "athena_query_scan_cutoff_bytes" {
  description = <<-EOT
    Per-query Athena scan cutoff in bytes. Cancellation can occur after the
    threshold is crossed, so this does not cap account-level query spending.
  EOT
  type        = number
  default     = 1073741824

  validation {
    condition = (
      var.athena_query_scan_cutoff_bytes >= 10485760 &&
      var.athena_query_scan_cutoff_bytes <= 10737418240 &&
      floor(var.athena_query_scan_cutoff_bytes) == var.athena_query_scan_cutoff_bytes
    )
    error_message = "athena_query_scan_cutoff_bytes must be a whole number from 10485760 through 10737418240."
  }
}

variable "ecr_retained_image_count" {
  description = "Maximum number of API images retained in ECR."
  type        = number
  default     = 10

  validation {
    condition     = var.ecr_retained_image_count >= 1 && floor(var.ecr_retained_image_count) == var.ecr_retained_image_count
    error_message = "ecr_retained_image_count must be a positive whole number."
  }
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

variable "enable_spine" {
  description = <<-EOT
    Creates the disposable Phase 1 queue, network, IAM loader role, and RDS
    warehouse. False retains the Phase 0 raw bucket while removing those costs.
  EOT
  type        = bool
  default     = false
}

variable "warehouse_publicly_accessible" {
  description = <<-EOT
    Gives the warehouse a public endpoint for temporary locally run Airflow
    access. Keep false unless a reviewed plan also supplies one developer /32.
  EOT
  type        = bool
  default     = false
}

variable "developer_ipv4_cidr" {
  description = <<-EOT
    The only IPv4 host allowed to reach a public warehouse. Must be null in
    private mode and one valid /32 CIDR when public access is enabled.
  EOT
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = var.warehouse_publicly_accessible ? (
      var.developer_ipv4_cidr != null &&
      can(cidrnetmask(var.developer_ipv4_cidr)) &&
      length(split(".", split("/", var.developer_ipv4_cidr)[0])) == 4 &&
      endswith(var.developer_ipv4_cidr, "/32")
    ) : var.developer_ipv4_cidr == null
    error_message = "developer_ipv4_cidr must be null in private mode or one valid IPv4 /32 CIDR in public mode."
  }
}
