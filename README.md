# GDPR Obfuscator Project
### Overview
GDPR Obfuscator is a general-purpose data processing tool designed to mask Personal Identifieable  Information (PII) in a file. It ensures compliance with GDPR regulations by anonymizing sensitive data (PII data) in a file and returns a bytestream representation of the file that compatible with AWS SDK (Boto3).

### Assumptions and Prerequisites
 1. Data is stored in *CSV*, *JSON*, or *Parquet* format in S3.
 2. Fields containing GDPR-sensitive data are known and will be supplied in 
advance.
 3. Data records will be supplied with a primary key.

### Features:
- Supports `.csv`, `.parquet`, and `.json` file formats.
- Serverless tool, built on **AWS Lambda**.
- Infrastructure managed as IaC using **Terraform**.
- Tested locally and in **CI/CD** pipelines.
 
### Architecture:
- AWS Lambda: Invoked by event bridge, step machine, etc. It calls the utility functions to perform the obfuscation process.   
- Utiltiy Functions: executes the logic of file obfuscation.
- IAM Roles and Policies: Manage access and permissions to lambda and s3 buckets.
- Terraform: automates deployment of the entire infrastructure.

### Data Flow:
1. File to obfuscate and PII fields sent to lambda handler.
2. Lambda function reads the file and PII fields.
3. Lambda function applies obfuscation to defined PII fields.
4. Obfuscated version of the file saved back to the s3 bucket.
5. Obfuscated file is converted to a bytestream representation of the file.
6. Lambda function returns bytestream of the file and file key as a response.

### Project Structure:
`````
gdpr-obfuscate/
├── .github/
│   └── workflows/
│       └── Deploy.yml               # GitHub Actions workflow for deployment
├── packages/
│   └── lambda_package.zip           # Lambda deployment package (auto-generated)
|
├── src/
│   ├── obfuscation_lambda.py        # Main Lambda handler function
│   ├── utils.py                     # Helper functions (Input parsing/data obfuscation)
│
├── terraform/
│   ├── main.tf                      # Defines AWS provider and Lambda setup
│   ├── vars.tf                      # Input variables for Terraform configuration
│   ├── lambda.tf                    # Lambda handler 
│   ├── iam.tf                       # IAM roles and permissions
|   
├── test/
│   ├── test_Obfuscation_lambda.py   # Unit tests for Lambda handler
│   ├── test_utils.py                # Unit tests for helper functions
│
├── Makefile                         # Automation for linting, testing, and packaging
├── requirements.txt                 # Python dependencies
├── README.md                        # Project documentation (this file)
└── .gitignore                       # Ignore files/folders for Git
`````
### Example Input / Output:
- Input json string:
    ```
    {
        "file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.csv",
        "pii_fields": ["name", "email_address"],
    }
    ```

- Input CSV File:
    ```
    student_id,name,course,cohort,graduation_date,email_address
    1234,John_Smith,Software,2024-03-31,2027-03-31,john@example.com
    ```
- Output:
    ```
    student_id,name,course,cohort,graduation_date,email_address
    ***,John_Smith,Software,2024-03-31,2027-03-31,***
    ```
    - *The output will be a bytestream representation of a file*
### Tech Stack:
- Version Control: Github.
- Infrastructure: Terraform.
- CI/CD: Github Actions, MakeFile, Yaml file. 
- Programming and Libraries: Python 3.11, boto3, pandas, coverage, black, bandit.
- AWS Services: S3, Lambda, IAM Role.
- Credentials: Stores in github secret.

### Infrastrucutre:
- AWS Provider.
- IAM Role Resource.
- IAM Policy Data and IAM Policy.
- Lambda Resource.
- External pandas resource.

### Testing:
- Run unit tests locally before deployment:
    ```python
    pytest -v
    ```
- *CI/CD pipelines automatically run these tests and deploy the Lambda if all pass.*

### Installation and Deployment:
1. Create an AWS Account and configure AWS credentials. The AWS user will need administrative privileges to set up the following: 
    - S3 Buckets.
    - Lambda Functions.
2. Ensure you have Python 3.11 installed.
    - Check your version: 
        ```python
        python --version
        ```
    - If needed, install it from [python.org](https://www.python.org/).
3. Store AWS Credentials in GitHub secrets.
4. Install Terraform (if not already installed).
5. Run Makefile Commands:
    - ```make all```  # Creates virtual environment, installs dependencies, formats code, runs security and test coverage checks
6. Set Up Terraform Backend.
    - Create an S3 bucket to store Terraform state files.
7. Deploy Infrastructure:
    ```cd terraform
    terraform init
    terraform plan
    terraform apply
    ```
- *If deploying via GitHub Actions, this step can be automated*

### License:
- This project is licensed under the MIT License.

### Contributers:
- Anas Elsafi – Data Engineer & Project Lead.


