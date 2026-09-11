locals {
  raw_source_schemas = {
    crashes     = jsondecode(file("${path.root}/../../ingest/schemas/crashes.json"))
    inspections = jsondecode(file("${path.root}/../../ingest/schemas/inspections.json"))
  }

  raw_catalog_tables = {
    for feed, schema in local.raw_source_schemas : feed => {
      name    = "raw_${feed}"
      columns = [for column_name in schema.columns : lower(column_name)]
    }
  }

  metadata_columns = [
    { name = "batch_id", type = "string" },
    { name = "content_sha256", type = "string" },
    { name = "object_sha256", type = "string" },
    { name = "observed_at", type = "string" },
    { name = "row_count", type = "bigint" },
    { name = "schema_fingerprint", type = "string" },
  ]
}

resource "aws_glue_catalog_database" "raw" {
  name        = "carrier_risk_raw_${var.environment}"
  description = "Explicit raw snapshot catalog for reproducible point-in-time analysis."
}

resource "aws_glue_catalog_table" "raw" {
  for_each = local.raw_catalog_tables

  database_name = aws_glue_catalog_database.raw.name
  name          = each.value.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL                 = "TRUE"
    classification           = "csv"
    compressionType          = "gzip"
    "skip.header.line.count" = "1"
  }

  partition_keys {
    name = "acquisition_date"
    type = "string"
  }

  storage_descriptor {
    compressed   = true
    input_format = "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat"
    location     = "s3://${aws_s3_bucket.raw.id}/catalog/feed=${each.key}/"
    output_format = (
      "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    )

    dynamic "columns" {
      for_each = each.value.columns

      content {
        name = columns.value
        type = "string"
      }
    }

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.serde2.OpenCSVSerde"

      parameters = {
        escapeChar    = "\\"
        quoteChar     = "\""
        separatorChar = ","
      }
    }
  }
}

resource "aws_glue_catalog_table" "snapshot_metadata" {
  database_name = aws_glue_catalog_database.raw.name
  name          = "raw_snapshot_metadata"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "json"
  }

  partition_keys {
    name = "feed"
    type = "string"
  }

  partition_keys {
    name = "acquisition_date"
    type = "string"
  }

  storage_descriptor {
    input_format = "org.apache.hadoop.mapred.TextInputFormat"
    location     = "s3://${aws_s3_bucket.raw.id}/catalog-metadata/"
    output_format = (
      "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    )

    dynamic "columns" {
      for_each = local.metadata_columns

      content {
        name = columns.value.name
        type = columns.value.type
      }
    }

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }
  }
}

resource "aws_glue_partition" "raw" {
  for_each = var.catalog_acquisition_dates

  database_name    = aws_glue_catalog_database.raw.name
  table_name       = aws_glue_catalog_table.raw[each.key].name
  partition_values = [each.value]

  storage_descriptor {
    compressed   = true
    input_format = "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat"
    location     = "s3://${aws_s3_bucket.raw.id}/catalog/feed=${each.key}/acquisition_date=${each.value}/"
    output_format = (
      "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    )

    dynamic "columns" {
      for_each = local.raw_catalog_tables[each.key].columns

      content {
        name = columns.value
        type = "string"
      }
    }

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.serde2.OpenCSVSerde"

      parameters = {
        escapeChar    = "\\"
        quoteChar     = "\""
        separatorChar = ","
      }
    }
  }
}

resource "aws_glue_partition" "snapshot_metadata" {
  for_each = var.catalog_acquisition_dates

  database_name    = aws_glue_catalog_database.raw.name
  table_name       = aws_glue_catalog_table.snapshot_metadata.name
  partition_values = [each.key, each.value]

  storage_descriptor {
    input_format = "org.apache.hadoop.mapred.TextInputFormat"
    location     = "s3://${aws_s3_bucket.raw.id}/catalog-metadata/feed=${each.key}/acquisition_date=${each.value}/"
    output_format = (
      "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    )

    dynamic "columns" {
      for_each = local.metadata_columns

      content {
        name = columns.value.name
        type = columns.value.type
      }
    }

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }
  }
}
