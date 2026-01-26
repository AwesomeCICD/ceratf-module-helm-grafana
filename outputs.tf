###############################################################################
# Outputs
###############################################################################

output "release_name" {
  description = "The name of the Helm release"
  value       = helm_release.grafana.name
}

output "release_namespace" {
  description = "The namespace where Grafana is deployed"
  value       = helm_release.grafana.namespace
}

output "release_version" {
  description = "The version of the Helm chart deployed"
  value       = helm_release.grafana.version
}

output "release_status" {
  description = "Status of the Helm release"
  value       = helm_release.grafana.status
}

output "grafana_service_name" {
  description = "The name of the Grafana Kubernetes service"
  value       = var.release_name
}

output "grafana_internal_url" {
  description = "Internal URL to access Grafana within the cluster"
  value       = "http://${var.release_name}.${var.namespace}.svc.cluster.local:80"
}

output "grafana_ingress_hosts" {
  description = "List of ingress hosts if ingress is enabled"
  value       = var.ingress_enabled ? var.ingress_hosts : []
}

###############################################################################
# PostgreSQL Outputs
###############################################################################

output "postgres_enabled" {
  description = "Whether PostgreSQL is enabled"
  value       = var.postgres_enabled
}

output "postgres_release_name" {
  description = "The name of the PostgreSQL Helm release"
  value       = var.postgres_enabled ? helm_release.postgresql[0].name : null
}

output "postgres_host" {
  description = "PostgreSQL hostname for internal cluster access"
  value       = var.postgres_enabled ? local.postgres_host : null
}

output "postgres_port" {
  description = "PostgreSQL port"
  value       = var.postgres_enabled ? local.postgres_port : null
}

output "postgres_database" {
  description = "PostgreSQL database name"
  value       = var.postgres_enabled ? var.postgres_database : null
}

output "postgres_username" {
  description = "PostgreSQL username"
  value       = var.postgres_enabled ? var.postgres_username : null
}

output "postgres_connection_string" {
  description = "PostgreSQL connection string (without password)"
  value       = var.postgres_enabled ? "postgresql://${var.postgres_username}@${local.postgres_host}:${local.postgres_port}/${var.postgres_database}" : null
}

output "postgres_internal_url" {
  description = "Internal URL to access PostgreSQL within the cluster"
  value       = var.postgres_enabled ? "postgresql://${local.postgres_host}:${local.postgres_port}" : null
}
