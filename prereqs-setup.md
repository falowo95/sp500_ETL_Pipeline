- ***Anaconda***
Download the installer:
```bash 
wget https://repo.anaconda.com/archive/Anaconda3-2021.11-Linux-x86_64.sh #Check for newer release

Run the installer and follow the prompts:

bash Anaconda3-2021.11-Linux-x86_64.sh #Press "yes" to everything

Activate the Anaconda base environment:
source .bashrc #To run up Anaconda base
```
- ***Docker + Docker-compose***
To install Docker and Docker-compose, follow these steps:
```bash
sudo apt-get install docker.io # Start with Docker installation. 

sudo groupadd docker
sudo gpasswd -a $USER docker
sudo service docker restart #Relogin after last command
```
```bash
mkdir bin/ # To collect binaries (executable apps)
cd bin
wget https://github.com/docker/compose/releases/download/v2.2.3/docker-compose-linux-x86_64 -O docker-compose
chmod +x docker-compose

nano .bashrc

# Add this stroke to the bottom of .bashrc and save it: $ export PATH="${HOME}/bin:${PATH}"

source .bashrc
```
- ***Terraform***
To install Terraform, follow these steps:
```bash
cd bin
wget https://releases.hashicorp.com/terraform/1.1.9/terraform_1.1.9_linux_amd64.zip #Check for newer release
unzip https://releases.hashicorp.com/terraform/1.1.9/terraform_1.1.9_linux_amd64.zip
rm https://releases.hashicorp.com/terraform/1.1.9/terraform_1.1.9_linux_amd64.zip
```

## GCP setup
1. Setup blank project at GCP. 
   **Note** that your "Project ID" would be used along the whole pipeline services. 
2. Setup [service account & authentication](https://cloud.google.com/docs/authentication/getting-started)
	-   Grant `Viewer` role to begin with.
	-   Download service-account-keys (.json) for auth.

### Acsess setup
To set up access:

1.  [IAM Roles](https://cloud.google.com/storage/docs/access-control/iam-roles) for Service account:
    -   Go to the _IAM_ section of _IAM & Admin_ [https://console.cloud.google.com/iam-admin/iam](https://console.cloud.google.com/iam-admin/iam)
    -   Click the _Edit principal_ icon for your service account.
    -   Add these roles in addition to _Viewer_ : **Storage Admin** + **Storage Object Admin** + **BigQuery Admin**
2.  Enable these APIs for your project:
    
    -   [https://console.cloud.google.com/apis/library/iam.googleapis.com](https://console.cloud.google.com/apis/library/iam.googleapis.com)
    - [https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com](https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com)
3. Create dir to store GCP credentials (would be used for running terraform and airflow)
```bash
cd ~ && mkdir -p ~/.google/credentials/
mv <path/to/your/service-account-authkeys>.json ~/.google/credentials/google_credentials.json 
```
