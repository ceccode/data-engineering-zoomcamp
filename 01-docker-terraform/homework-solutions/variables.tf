variable "project_id" {
  description = "The ID of the GCP project"
  default     = "datazoomcamp-2025"
}

variable "project_region" {
  description = "Region of the project"
  default     = "ueurope-west12-a"
}

variable "location" {
  description = "Project location"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "BigQuery dataset name"
  default     = "datazoomcamp_dataset"
}

variable "gcs_bucket_name" {
  description = "My storage bucket name"
  default     = "datazoomcamp_bucket"
}

variable "gcs_storage_class" {
  description = "Bucket storage class"
  default     = "STANDARD"
}

variable "credentials_file" {
  description = "Path to the GCP credentials JSON file"
  default     = "./.keys/my-creds.json"
}