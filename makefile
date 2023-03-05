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

###################################################################################################################### 
# Set up local infrastructure


#######################################################################################################################