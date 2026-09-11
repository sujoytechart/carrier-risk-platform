# Queryable derivatives are separate from immutable source snapshots. Columns
# retain the validated source order; positional Parquet access handles uppercase
# federal headers without changing their representation during conversion.
locals {
  derived_metadata_columns = concat(local.metadata_columns, [
    { name = "converter_version", type = "string" },
    { name = "parquet_sha256", type = "string" },
    { name = "values_sha256", type = "string" },
  ])
}

resource "aws_glue_catalog_table" "derived" {
  for_each      = local.raw_catalog_tables
  database_name = aws_glue_catalog_database.raw.name
  name          = "derived_${each.key}"
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }
  partition_keys {
    name = "acquisition_date"
    type = "string"
  }
  storage_descriptor {
    location      = "s3://${aws_s3_bucket.raw.id}/derived/v1/feed=${each.key}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    dynamic "columns" {
      for_each = each.value.columns
      content {
        name = columns.value
        type = "string"
      }
    }
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "parquet.column.index.access" = "true"
      }
    }
  }
}

resource "aws_glue_partition" "derived" {
  for_each         = var.catalog_acquisition_dates
  database_name    = aws_glue_catalog_database.raw.name
  table_name       = aws_glue_catalog_table.derived[each.key].name
  partition_values = [each.value]
  storage_descriptor {
    location      = "s3://${aws_s3_bucket.raw.id}/derived/v1/feed=${each.key}/acquisition_date=${each.value}/data/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    dynamic "columns" {
      for_each = local.raw_catalog_tables[each.key].columns
      content {
        name = columns.value
        type = "string"
      }
    }
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "parquet.column.index.access" = "true"
      }
    }
  }
}

resource "aws_glue_catalog_table" "derived_metadata" {
  database_name = aws_glue_catalog_database.raw.name
  name          = "derived_snapshot_metadata"
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
    location      = "s3://${aws_s3_bucket.raw.id}/derived-metadata/v1/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    dynamic "columns" {
      for_each = local.derived_metadata_columns
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

resource "aws_glue_partition" "derived_metadata" {
  for_each         = var.catalog_acquisition_dates
  database_name    = aws_glue_catalog_database.raw.name
  table_name       = aws_glue_catalog_table.derived_metadata.name
  partition_values = [each.key, each.value]
  storage_descriptor {
    location      = "s3://${aws_s3_bucket.raw.id}/derived-metadata/v1/feed=${each.key}/acquisition_date=${each.value}/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    dynamic "columns" {
      for_each = local.derived_metadata_columns
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
