###############################################################################
# Provider Configuration for Local Testing
# NOTE: When using this as a module, providers should be configured in the
#       root module, not here. This file is for local testing only.
###############################################################################

provider "kubernetes" {
  config_path    = "/Users/derry_1/.kube/config"
  config_context = "docker-desktop"
}

# Helm provider 3.x syntax (uses = instead of block)
provider "helm" {
  kubernetes = {
    config_path    = "/Users/derry_1/.kube/config"
    config_context = "docker-desktop"
  }
}
