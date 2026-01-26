###############################################################################
# Grafana Helm Deployment
# Deploys Grafana to a Kubernetes cluster using the official Helm chart
###############################################################################

###############################################################################
# Local Values
###############################################################################

locals {
  # PostgreSQL connection details
  postgres_host     = var.postgres_enabled ? "${var.postgres_release_name}-postgresql.${var.namespace}.svc.cluster.local" : null
  postgres_port     = 5432
  postgres_password = var.postgres_password != null ? var.postgres_password : random_password.postgres[0].result

  # PostgreSQL Helm chart values
  postgres_values = {
    # Override image to use a verified working tag
    image = {
      registry   = "docker.io"
      repository = "bitnami/postgresql"
      tag        = "latest"
    }

    auth = {
      username = var.postgres_username
      password = local.postgres_password
      database = var.postgres_database
    }

    primary = {
      persistence = {
        enabled      = var.postgres_persistence_enabled
        size         = var.postgres_persistence_size
        storageClass = var.postgres_persistence_storage_class
      }

      resources = var.postgres_resources
    }

    # Disable metrics by default for simpler setup
    metrics = {
      enabled = false
    }
  }

  postgres_merged_values = merge(local.postgres_values, var.postgres_extra_values)

  # Grafana database configuration when PostgreSQL is enabled
  grafana_database_config = var.postgres_enabled ? {
    "grafana.ini" = {
      database = {
        type     = "postgres"
        host     = "${local.postgres_host}:${local.postgres_port}"
        name     = var.postgres_database
        user     = var.postgres_username
        password = local.postgres_password
        ssl_mode = "disable"
      }
    }
  } : {}

  # Generate chart values from input variables
  chart_values = {
    # Disable secret leak detection (we're passing DB password via values)
    assertNoLeakedSecrets = false

    # Use a reliable image tag
    image = {
      repository = "grafana/grafana"
      tag        = "latest"
    }

    # Fix init container image
    initChownData = {
      image = {
        repository = "busybox"
        tag        = "latest"
      }
    }

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

  # Merge with database config and any extra values provided by the user
  merged_values = merge(local.chart_values, local.grafana_database_config, var.extra_values)
}

###############################################################################
# Random Password for PostgreSQL (if not provided)
###############################################################################

resource "random_password" "postgres" {
  count   = var.postgres_enabled && var.postgres_password == null ? 1 : 0
  length  = 24
  special = false
}

###############################################################################
# PostgreSQL Helm Release
###############################################################################

resource "helm_release" "postgresql" {
  count = var.postgres_enabled ? 1 : 0

  name       = var.postgres_release_name
  repository = "https://charts.bitnami.com/bitnami"
  chart      = "postgresql"
  version    = var.postgres_chart_version
  namespace  = var.namespace

  create_namespace = var.create_namespace
  timeout          = var.timeout
  atomic           = var.atomic
  wait             = true # Always wait for PostgreSQL to be ready

  values = [yamlencode(local.postgres_merged_values)]
}

###############################################################################
# Grafana Helm Release
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

  # Ensure PostgreSQL is ready before deploying Grafana
  depends_on = [helm_release.postgresql]
}
