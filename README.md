# bq-metadata-cloud-run

Cloud Run job to pull metadata manifests from Synapse and update tables in the Google BigQuery dataset `htan-dcc.combined_assays`. This dataset contains clinical, biospecimen, and assay metadata tables combined across HTAN centers. 

The Cloud Run job is scheduled to run weekly on Mondays at 0300 ET.

## Requirements
Requires download access to individual HTAN-center Synapse projects. 

Requires access to deploy resources in the HTAN Google Cloud Project, `htan-dcc`. Please contact an owner of `htan-dcc` to request access (Owners in 2024: Clarisse Lau, Vesteinn Thorsson, William Longabaugh, ISB)

## Prerequisites
- Create a [Synapse Auth Token secret](https://help.synapse.org/docs/Managing-Your-Account.2055405596.html#ManagingYourAccount-PersonalAccessTokens) in [Secret Manager](https://cloud.google.com/secret-manager/docs)
- Install [Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli) 

## Docker Image
Before creating job, build and push a docker image to Google Artifact Registry (recommended)

```
cd code
docker build . -t gcr.io/<gc-project>/<image-name>
docker push gcr.io/<gc-project>/<image-name>
```

## Deploy Cloud Resources
Define variables in [terraform.tfvars](https://github.com/ncihtan/bq-metadata-cloud-run/blob/main/terraform.tfvars). Variable descriptions can be found in [variables.tf](https://github.com/ncihtan/bq-metadata-cloud-run/blob/main/variables.tf)

```
terraform init
terraform plan
terraform apply
```
