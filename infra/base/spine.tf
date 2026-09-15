locals {
  common_tags = {
    Project     = "carrier-risk-platform"
    ManagedBy   = "terraform"
    Environment = var.environment
  }
}

# The event spine is intentionally disposable. Disabling this module removes
# the warehouse, queues, and network while leaving the Phase 0 raw evidence in
# this root intact.
module "spine" {
  count  = var.enable_spine ? 1 : 0
  source = "./modules/spine"

  account_id          = data.aws_caller_identity.current.account_id
  common_tags         = local.common_tags
  developer_ipv4_cidr = var.developer_ipv4_cidr
  environment         = var.environment
  loader_principal_arns = distinct(concat(
    [data.aws_iam_role.terraform_operator.arn],
    var.additional_loader_principals,
  ))
  publicly_accessible = var.warehouse_publicly_accessible
  raw_bucket_arn      = aws_s3_bucket.raw.arn
  raw_bucket_id       = aws_s3_bucket.raw.id
}
