variable "aws_region" {
  description = "AWS Region"
  type        = string
  default     = "eu-north-1"
}


variable "project_name" {
  description = "Project name for this CDE project"
  type        = string
  default     = "cde-telecoms-project"
}


variable "password" {
  type      = string
  sensitive = true
}

variable "snowflake_admin_role" { 
  type = string
  default = "ACCOUNTADMIN" 
}