#################################################################################
#
# Makefile to build the project
#
#################################################################################

PROJECT_NAME = gdpr-obfuscation-project
REGION = us-east-1
PYTHON_INTERPRETER = python3
WD=$(shell pwd)
PYTHONPATH=${WD}
SHELL := /bin/bash
PROFILE = default
PIP:=pip3

## Create python interpreter environment.
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up VirtualEnv."
	( \
	    $(PIP) install -q virtualenv virtualenvwrapper; \
	    virtualenv venv --python=$(PYTHON_INTERPRETER); \
	)

# Define utility variable to help calling Python from the virtual environment
ACTIVATE_ENV := source venv/bin/activate

# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

## Set up log directory
logdirs:
	mkdir -p logs

## Build the environment requirements
requirements: create-environment logdirs
	python -m venv venv
	python -m pip install -r requirements.txt
	

################################################################################################################
# Set Up
## Install bandit
bandit:
	python -m venv venv
	source venv/bin/activate && pip install bandit && bandit -r src


## Install black
black:
	python -m venv venv
	source venv/bin/activate && pip install black 
## Install coverage
coverage:
	python -m venv venv
	source venv/bin/activate && pip install coverage 
# Set up dev requirements (bandit, black, coverage)
dev-setup: bandit black coverage

# Build / Run

## Run the security test (bandit)
security-test:
	python -m bandit -lll */*.py *c/*.py
## Run the black code check
run-black:
## Run the unit testsrun-black:
	python -m black src test
	
## Run the unit tests
unit-test:
	pip install moto[boto3]==5.0.7
	PYTHONPATH=$(PWD) python -m pytest -vvv
## Run the coverage check
check-coverage:
	python -m venv venv
	python -m pip install --upgrade pip
	python -m pip install pytest coverage
	PYTHONPATH=$(PWD) python -m coverage run --omit 'venv/*' -m pytest
	PYTHONPATH=$(PWD) python -m coverage report -m
## Run all checks

run-checks: security-test run-black unit-test check-coverage

all: requirements dev-setup run-checks