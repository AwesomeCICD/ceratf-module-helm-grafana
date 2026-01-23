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
