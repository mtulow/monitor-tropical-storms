
# Data Lake Bucket
output "data_lake_bucket" {
    value = google_storage_bucket.data-lake-bucket.name
}

# Data Warehouse
output "data_warehouse_name" {
    value = google_bigquery_dataset.dataset.dataset_id
}