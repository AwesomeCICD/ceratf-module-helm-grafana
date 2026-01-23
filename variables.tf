###############################################################################
# Required Variables
###############################################################################

variable "namespace" {
  description = "Kubernetes namespace where Grafana will be deployed"
  type        = string
}

variable "release_name" {
  description = "Helm release name for the Grafana deployment"
  type        = string
  default     = "grafana"
}

###############################################################################
# Optional Variables
###############################################################################

variable "chart_version" {
  description = "Version of the Grafana Helm chart to deploy"
  type        = string
  default     = "8.8.2"
}

variable "create_namespace" {
  description = "Whether to create the namespace if it doesn't exist"
  type        = bool
  default     = true
}

variable "admin_user" {
  description = "Grafana admin username"
  type        = string
  default     = "admin"
}

variable "admin_password" {
  description = "Grafana admin password. If not set, a random password will be generated"
  type        = string
  default     = null
  sensitive   = true
}

variable "persistence_enabled" {
  description = "Enable persistent storage for Grafana"
  type        = bool
  default     = true
}

variable "persistence_size" {
  description = "Size of the persistent volume for Grafana"
  type        = string
  default     = "10Gi"
}

variable "persistence_storage_class" {
  description = "Storage class for the persistent volume"
  type        = string
  default     = null
}

variable "ingress_enabled" {
  description = "Enable ingress for Grafana"
  type        = bool
  default     = false
}

variable "ingress_hosts" {
  description = "List of ingress hosts for Grafana"
  type        = list(string)
  default     = []
}

variable "ingress_class_name" {
  description = "Ingress class name"
  type        = string
  default     = "nginx"
}

variable "ingress_tls_enabled" {
  description = "Enable TLS for ingress"
  type        = bool
  default     = false
}

variable "ingress_tls_secret_name" {
  description = "TLS secret name for ingress"
  type        = string
  default     = ""
}

variable "service_type" {
  description = "Kubernetes service type for Grafana (ClusterIP, NodePort, LoadBalancer)"
  type        = string
  default     = "ClusterIP"
}

variable "replicas" {
  description = "Number of Grafana replicas"
  type        = number
  default     = 1
}

variable "resources" {
  description = "Resource requests and limits for Grafana pods"
  type = object({
    requests = optional(object({
      cpu    = optional(string, "100m")
      memory = optional(string, "128Mi")
    }), {})
    limits = optional(object({
      cpu    = optional(string, "500m")
      memory = optional(string, "512Mi")
    }), {})
  })
  default = {}
}

variable "datasources" {
  description = "List of datasources to configure in Grafana"
  type = list(object({
    name       = string
    type       = string
    url        = string
    access     = optional(string, "proxy")
    is_default = optional(bool, false)
    json_data  = optional(map(string), {})
  }))
  default = []
}

variable "dashboards_provider_enabled" {
  description = "Enable dashboard provider for loading dashboards from ConfigMaps"
  type        = bool
  default     = false
}

variable "plugins" {
  description = "List of Grafana plugins to install"
  type        = list(string)
  default     = []
}

variable "extra_values" {
  description = "Extra values to pass to the Helm chart (will be merged with generated values)"
  type        = any
  default     = {}
}

variable "common_tags" {
  description = "Map of tags that will be applied to all resources"
  type        = map(string)
  default     = {}
}

variable "timeout" {
  description = "Timeout for Helm operations in seconds"
  type        = number
  default     = 300
}

variable "atomic" {
  description = "If true, installation process purges chart on fail"
  type        = bool
  default     = true
}

variable "wait" {
  description = "Will wait until all resources are in a ready state before marking the release as successful"
  type        = bool
  default     = true
}
