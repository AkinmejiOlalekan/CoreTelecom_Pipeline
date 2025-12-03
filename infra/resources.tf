
resource "aws_s3_bucket" "migration" {
  bucket = "cde-telecom-unified-bucket"

  tags = local.constant_tags
}


resource "aws_iam_role" "snowflake_role" {
  name = "snowflake_s3_reader_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::714551764970:user/1sx71000-s"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = "ZY54618_SFCRole=4_zO4Ct/2uT7MRJpotwcSiFtAEPuc="
          }
        }
      }
    ]
  })
}

data "aws_iam_policy_document" "snowflake_role_policy" {
  statement {
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:PutObject",
      "s3:GetBucketLocation"
    ]
    resources = [
      aws_s3_bucket.migration.arn,
      "${aws_s3_bucket.migration.arn}/*"
    ]
  }
  statement {
      actions = [
        "s3:GetObject",
        "s3:ListMultipartUploadParts"
      ]
      resources = [
        "${aws_s3_bucket.migration.arn}/*"
      ]
    }
}

resource "aws_iam_policy" "s3_read_policy" {
  name   = "snowflake_dedicated_policy"
  policy = data.aws_iam_policy_document.snowflake_role_policy.json
}

resource "aws_iam_role_policy_attachment" "attach" {
  role = aws_iam_role.snowflake_role.name
  policy_arn = aws_iam_policy.s3_read_policy.arn
}

resource "snowflake_file_format" "parquet" {
  name     = "MY_PARQUET_FORMAT"
  database = "CORE_TELECOM_DB"
  schema   = "STAGING"
  format_type = "PARQUET"
  null_if = ["NULL", ""]
  compression = "AUTO"
}

resource "snowflake_storage_integration" "integration" {
  name                      = "S3_WAREHOUSE_INTEGRATION"
  type                      = "EXTERNAL_STAGE"
  enabled                   = true
  storage_allowed_locations = ["s3://cde-telecom-unified-bucket/staging/"]

  storage_provider     = "S3"
  storage_aws_role_arn = aws_iam_role.snowflake_role.arn
}

resource "snowflake_stage" "s3_parquet_stage" {
  name                = "TELECOM_SNOWFLAKE_STAGE"
  url                 = "s3://cde-telecom-unified-bucket/staging/"
  database            = "CORE_TELECOM_DB"
  schema              = "STAGING"
  storage_integration = snowflake_storage_integration.integration.name
  file_format = "format_name='${snowflake_file_format.parquet.database}.${snowflake_file_format.parquet.schema}.${snowflake_file_format.parquet.name}'"

  depends_on = [
    snowflake_storage_integration.integration,
    snowflake_file_format.parquet
  ]
}