# bq-metadata-cloud-run

Cloud Run job to pull metadata manifests from Synapse and update tables in the Google BigQuery dataset `htan-dcc.combined_assays`. This dataset contains clinical, biospecimen, and assay metadata tables combined across HTAN centers. 

The Cloud Run job is scheduled to run weekly on Mondays at 0300 ET.

# Requirements
Requires download access to individual HTAN-center Synapse projects. 

Requires access to deploy resources in the HTAN Google Cloud Project, `htan-dcc`. Please contact an owner of `htan-dcc` to request access (Owners in 2024: Clarisse Lau, Vesteinn Thorsson, William Longabaugh, ISB)

# Prerequisites
- Create SYNAPSE_AUTH_TOKEN secret in Secret Manager
- Terraform 

# Docker Image

```
cd code
docker build . -t gcr.io/<gc-project>/<image-name>
docker push gcr.io/<gc-project>/<image-name>
```

# Deploy Cloud Run Job

```
terraform init
terraform plan
terraform apply
```
