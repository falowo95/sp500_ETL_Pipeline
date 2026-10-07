##################################################################################################
# Set up cloud infrastructure
#
# One-time bootstrap (creates the Terraform state bucket itself — run once,
# by hand, before anything below):
#   terraform -chdir=terraform/bootstrap init
#   terraform -chdir=terraform/bootstrap apply -var="project=dataengineering-378316"

tf-init:
	terraform -chdir=terraform init

infra-up:
	terraform -chdir=terraform apply

infra-down:
	terraform -chdir=terraform destroy

infra-config:
	terraform -chdir=terraform output
