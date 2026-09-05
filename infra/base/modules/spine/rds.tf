resource "aws_db_subnet_group" "warehouse" {
  name       = "carrier-risk-warehouse-${var.environment}"
  subnet_ids = aws_subnet.warehouse[*].id
  tags       = var.common_tags
}

# Raw snapshots are the durable recovery source. This database is deliberately
# reconstructible and disposable so operators can remove its running cost.
resource "aws_db_instance" "warehouse" {
  identifier = "carrier-risk-warehouse-${var.environment}"

  allocated_storage            = 20
  auto_minor_version_upgrade   = true
  backup_retention_period      = 1
  db_name                      = "carrier_risk"
  db_subnet_group_name         = aws_db_subnet_group.warehouse.name
  delete_automated_backups     = true
  deletion_protection          = false
  engine                       = "postgres"
  engine_version               = "17"
  instance_class               = "db.t4g.micro"
  manage_master_user_password  = true
  monitoring_interval          = 0
  multi_az                     = false
  performance_insights_enabled = false
  port                         = 5432
  publicly_accessible          = var.publicly_accessible
  skip_final_snapshot          = true
  storage_encrypted            = true
  storage_type                 = "gp3"
  username                     = "carrier_risk_admin"
  vpc_security_group_ids       = [aws_security_group.warehouse.id]
  tags                         = var.common_tags
}
