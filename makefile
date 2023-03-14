down: 
	docker compose down
up:
	docker-compose up -d


####################################################################################################################
# Set up cloud infrastructure

tf-init:
	terraform -chdir=./terraform init

infra-up:
	terraform -chdir=./terraform apply

infra-down:
	terraform -chdir=./terraform destroy

infra-config:
	terraform -chdir=./terraform output

ssh-google_vm:
	gcloud compute ssh --zone "europe-west1-b" "de-zoomcamp"  --project "dataengineering-378316"

ssh-de-zoomcamp:
	ssh de-zoomcamp
###################################################################################################################### 
# Set up local infrastructure
