terraform {
  required_version = ">= 1.10"

  backend "s3" {
    encrypt      = true
    key          = "carrier-risk/base/terraform.tfstate"
    use_lockfile = true
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # The bucket and region remain partial so offline initialization can use
  # -backend=false and deployment can supply backend.hcl explicitly.
}
