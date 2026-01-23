###############################################################################
# Grafana Helm Deployment
# Deploys Grafana to a Kubernetes cluster using the official Helm chart
###############################################################################

###############################################################################
# Local Values
###############################################################################

locals {
  # Generate chart values from input variables
  chart_values = {
    replicas = var.replicas

    adminUser     = var.admin_user
    adminPassword = var.admin_password

    persistence = {
      enabled          = var.persistence_enabled
      size             = var.persistence_size
      storageClassName = var.persistence_storage_class
    }

    service = {
      type = var.service_type
    }

    ingress = {
      enabled          = var.ingress_enabled
      ingressClassName = var.ingress_class_name
      hosts            = var.ingress_hosts
      tls = var.ingress_tls_enabled ? [
        {
          secretName = var.ingress_tls_secret_name
          hosts      = var.ingress_hosts
        }
      ] : []
    }

    resources = var.resources

    plugins = var.plugins

    datasources = length(var.datasources) > 0 ? {
      "datasources.yaml" = {
        apiVersion = 1
        datasources = [
          for ds in var.datasources : {
            name      = ds.name
            type      = ds.type
            url       = ds.url
            access    = ds.access
            isDefault = ds.is_default
            jsonData  = ds.json_data
          }
        ]
      }
    } : {}

    sidecar = var.dashboards_provider_enabled ? {
      dashboards = {
        enabled = true
      }
    } : {}

    podLabels = var.common_tags
  }

  # Merge with any extra values provided by the user
  merged_values = merge(local.chart_values, var.extra_values)
}

###############################################################################
# Helm Release
###############################################################################

resource "helm_release" "grafana" {
  name       = var.release_name
  repository = "https://grafana.github.io/helm-charts"
  chart      = "grafana"
  version    = var.chart_version
  namespace  = var.namespace

  create_namespace = var.create_namespace
  timeout          = var.timeout
  atomic           = var.atomic
  wait             = var.wait

  values = [yamlencode(local.merged_values)]
}
