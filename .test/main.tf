module "grafana" {
  source = "../"

  namespace    = "test-grafana"
  release_name = "grafana-test"

  postgres_enabled = false

  admin_user     = "admin"
  admin_password = "test-password"

  persistence_enabled = false
}
