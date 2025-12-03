terraform {
  backend "s3" {
    bucket = "cde-telecom-unified-bucket"
    key    = "state/terraform.tfstate"
    region = "eu-north-1"
  }
}
