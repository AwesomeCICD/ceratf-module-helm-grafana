###############################################################################
# Terraform and Provider Version Constraints
###############################################################################

terraform {
  required_version = ">= 1.0.9"

  required_providers {
    helm = {
      source  = "hashicorp/helm"
      version = ">= 2.0.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.0.0"
    }
  }
}
