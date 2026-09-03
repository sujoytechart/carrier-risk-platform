provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = "carrier-risk-platform"
      ManagedBy   = "terraform"
      Environment = var.environment
    }
  }
}

data "aws_caller_identity" "current" {}
