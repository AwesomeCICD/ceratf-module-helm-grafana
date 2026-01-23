# ceratf-module-helm-grafana

Deploys Grafana to a Kubernetes cluster using the official Helm chart. Does the following:

- Deploys Grafana using the official Grafana Helm chart
- Configures persistent storage for dashboards and settings
- Optionally configures ingress for external access
- Supports datasource and dashboard provisioning
- Supports plugin installation

## Requirements

- Terraform >= 1.0.9
- Helm provider >= 2.0.0
- Kubernetes provider >= 2.0.0
- Access to a Kubernetes cluster with Helm capabilities

## How to Use

1. Drop the example module declaration shown below into a Terraform plan and fill in the variables.
2. Ensure your Kubernetes and Helm providers are configured.
3. Run the Terraform plan.

## Terraform Variables

### Required

| Name         | Default   | Description                                       |
| ------------ | --------- | ------------------------------------------------- |
| namespace    | none      | Kubernetes namespace where Grafana will be deployed |
| release_name | "grafana" | Helm release name for the Grafana deployment     |

### Optional

| Name                      | Default     | Description                                                         |
| ------------------------- | ----------- | ------------------------------------------------------------------- |
| chart_version             | "8.8.2"     | Version of the Grafana Helm chart to deploy                        |
| create_namespace          | true        | Whether to create the namespace if it doesn't exist                |
| admin_user                | "admin"     | Grafana admin username                                              |
| admin_password            | null        | Grafana admin password. If not set, a random password is generated |
| persistence_enabled       | true        | Enable persistent storage for Grafana                              |
| persistence_size          | "10Gi"      | Size of the persistent volume for Grafana                          |
| persistence_storage_class | null        | Storage class for the persistent volume                            |
| ingress_enabled           | false       | Enable ingress for Grafana                                          |
| ingress_hosts             | []          | List of ingress hosts for Grafana                                  |
| ingress_class_name        | "nginx"     | Ingress class name                                                  |
| ingress_tls_enabled       | false       | Enable TLS for ingress                                              |
| ingress_tls_secret_name   | ""          | TLS secret name for ingress                                         |
| service_type              | "ClusterIP" | Kubernetes service type (ClusterIP, NodePort, LoadBalancer)        |
| replicas                  | 1           | Number of Grafana replicas                                          |
| resources                 | {}          | Resource requests and limits for Grafana pods                      |
| datasources               | []          | List of datasources to configure in Grafana                        |
| dashboards_provider_enabled | false     | Enable dashboard provider for loading dashboards from ConfigMaps   |
| plugins                   | []          | List of Grafana plugins to install                                  |
| extra_values              | {}          | Extra values to pass to the Helm chart                             |
| common_tags               | {}          | Map of tags that will be applied to all resources                  |
| timeout                   | 300         | Timeout for Helm operations in seconds                              |
| atomic                    | true        | If true, installation process purges chart on fail                 |
| wait                      | true        | Wait until all resources are ready before marking release successful|

## Terraform Outputs

| Name                  | Description                                           |
| --------------------- | ----------------------------------------------------- |
| release_name          | The name of the Helm release                          |
| release_namespace     | The namespace where Grafana is deployed               |
| release_version       | The version of the Helm chart deployed                |
| release_status        | Status of the Helm release                            |
| grafana_service_name  | The name of the Grafana Kubernetes service            |
| grafana_internal_url  | Internal URL to access Grafana within the cluster     |
| grafana_ingress_hosts | List of ingress hosts if ingress is enabled           |

## Example Usage

### Basic Deployment

```hcl
module "grafana" {
  source = "git@github.com:AwesomeCICD/ceratf-module-helm-grafana.git"

  namespace    = "monitoring"
  release_name = "grafana"
}
```

### With Ingress and Prometheus Datasource

```hcl
module "grafana" {
  source = "git@github.com:AwesomeCICD/ceratf-module-helm-grafana.git"

  namespace    = "monitoring"
  release_name = "grafana"

  ingress_enabled         = true
  ingress_hosts           = ["grafana.example.com"]
  ingress_tls_enabled     = true
  ingress_tls_secret_name = "grafana-tls"

  datasources = [
    {
      name       = "Prometheus"
      type       = "prometheus"
      url        = "http://prometheus-server.monitoring.svc.cluster.local"
      is_default = true
    }
  ]

  plugins = [
    "grafana-piechart-panel",
    "grafana-clock-panel"
  ]

  common_tags = {
    Environment = "production"
    Team        = "platform"
  }
}
```

### With Custom Resources and Storage

```hcl
module "grafana" {
  source = "git@github.com:AwesomeCICD/ceratf-module-helm-grafana.git"

  namespace    = "monitoring"
  release_name = "grafana"

  persistence_enabled       = true
  persistence_size          = "20Gi"
  persistence_storage_class = "gp3"

  resources = {
    requests = {
      cpu    = "200m"
      memory = "256Mi"
    }
    limits = {
      cpu    = "1000m"
      memory = "1Gi"
    }
  }
}
```

### Provider Configuration

Ensure you have the Helm and Kubernetes providers configured in your root module:

```hcl
provider "kubernetes" {
  config_path = "~/.kube/config"
}

provider "helm" {
  kubernetes {
    config_path = "~/.kube/config"
  }
}
```

## Resources Created by Terraform

- `helm_release.grafana` - Grafana Helm release

The Helm chart creates the following Kubernetes resources:
- Deployment (Grafana pods)
- Service (ClusterIP, NodePort, or LoadBalancer)
- ConfigMaps (for datasources, dashboards, etc.)
- Secrets (for admin credentials)
- PersistentVolumeClaim (if persistence is enabled)
- Ingress (if ingress is enabled)
- ServiceAccount
- Role and RoleBinding (if RBAC is enabled)

## About

Deploys Grafana to a Kubernetes cluster using the official Helm chart for observability and dashboarding needs.
