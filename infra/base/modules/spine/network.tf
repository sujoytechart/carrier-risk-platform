data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "warehouse" {
  cidr_block           = "10.42.0.0/24"
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags                 = merge(var.common_tags, { Name = "carrier-risk-${var.environment}" })
}

resource "aws_subnet" "warehouse" {
  count = 2

  availability_zone       = data.aws_availability_zones.available.names[count.index]
  cidr_block              = cidrsubnet(aws_vpc.warehouse.cidr_block, 4, count.index)
  map_public_ip_on_launch = false
  vpc_id                  = aws_vpc.warehouse.id
  tags = merge(var.common_tags, {
    Name = "carrier-risk-${var.environment}-${count.index + 1}"
  })
}

# Internet routing exists only for the explicitly reviewed local-Airflow mode.
# Private mode creates neither an internet gateway nor a default route.
resource "aws_internet_gateway" "public" {
  count  = var.publicly_accessible ? 1 : 0
  vpc_id = aws_vpc.warehouse.id
  tags   = var.common_tags
}

resource "aws_route_table" "public" {
  count  = var.publicly_accessible ? 1 : 0
  vpc_id = aws_vpc.warehouse.id
  tags   = var.common_tags

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.public[0].id
  }
}

resource "aws_route_table_association" "public" {
  count = var.publicly_accessible ? 2 : 0

  route_table_id = aws_route_table.public[0].id
  subnet_id      = aws_subnet.warehouse[count.index].id
}

resource "aws_security_group" "warehouse" {
  name        = "carrier-risk-warehouse-${var.environment}"
  description = "PostgreSQL ingress is absent unless one developer /32 is approved."
  vpc_id      = aws_vpc.warehouse.id
  tags        = var.common_tags
}

resource "aws_vpc_security_group_ingress_rule" "developer" {
  count = var.publicly_accessible ? 1 : 0

  cidr_ipv4         = var.developer_ipv4_cidr
  description       = "Temporary PostgreSQL access for the reviewed developer host."
  from_port         = 5432
  ip_protocol       = "tcp"
  security_group_id = aws_security_group.warehouse.id
  to_port           = 5432
  tags              = var.common_tags
}
