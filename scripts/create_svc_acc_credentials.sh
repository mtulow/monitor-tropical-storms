#!/bin/bash

# Create service account for infra-sa
gcloud iam service-accounts create infra-sa


# Grant the following roles to the service account:
# Viewer
gcloud projects \
    add-iam-policy-binding monitor-tropical-storms \
    --member="serviceAccount:infra-sa@monitor-tropical-storms.iam.gserviceaccount.com" \
    --role=roles/viewer

# BigQuery Admin
gcloud projects \
    add-iam-policy-binding monitor-tropical-storms \
    --member="serviceAccount:infra-sa@monitor-tropical-storms.iam.gserviceaccount.com" \
    --role=roles/bigquery.admin

# Storage Admin
gcloud projects \
    add-iam-policy-binding monitor-tropical-storms \
    --member="serviceAccount:infra-sa@monitor-tropical-storms.iam.gserviceaccount.com" \
    --role=roles/storage.admin

# Storage Object Admin
gcloud projects \
    add-iam-policy-binding monitor-tropical-storms \
    --member="serviceAccount:infra-sa@monitor-tropical-storms.iam.gserviceaccount.com" \
    --role=roles/storage.objectAdmin

# Grant your Account a role that lets you use the service account's 
# roles and attach the service account to other resources:
gcloud iam service-accounts \
    add-iam-policy-binding infra-sa@monitor-tropical-storms.iam.gserviceaccount.com \
    --member="user:mtulow@icloud.com" \
    --role=roles/iam.serviceAccountUser

# Service Account Key
gcloud iam service-accounts \
    keys create infra-sa-key.json \
    --iam-account=infra-sa@monitor-tropical-storms.iam.gserviceaccount.com


