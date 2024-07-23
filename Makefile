# Define variables
VENV_DIR = venv
PYTHON = python3.12
PIP = $(VENV_DIR)/bin/pip

####################################################################################################################
# Setup local env to run EDA scripts
# Default target
all: setup

# Target to set up the virtual environment and install packages
setup: $(VENV_DIR)/bin/activate

$(VENV_DIR)/bin/activate: $(VENV_DIR)
	$(PYTHON) -m venv $(VENV_DIR)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	touch $(VENV_DIR)/bin/activate

$(VENV_DIR):
	$(PYTHON) -m venv $(VENV_DIR)

# Target to activate the virtual environment
activate:
	@echo "Run 'source $(VENV_DIR)/bin/activate' to activate the virtual environment."

# Target to clean up the virtual environment
clean:
	rm -rf $(VENV_DIR)

.PHONY: all setup activate clean

####################################################################################################################
# Setup containers to run Airflow and stand up Data warehouse as per requirements

docker-spin-up:
	docker compose up airflow-init && docker compose up --build -d

perms:
	sudo mkdir -p logs plugins temp dags tests migrations data visualization && sudo chmod -R u=rwx,g=rwx,o=rwx logs plugins temp dags tests migrations data visualization

up: perms docker-spin-up 

down:
	docker compose down --volumes --rmi all

restart: down up

sh:
	docker exec -ti webserver bash

psql: # This postgres container is used to manage metadata for airflow
	docker exec -ti postgres psql -U airflow -W airflow

psql-warehouse: # This postgres container is used for our datawarehouse
	docker exec -ti postgres_warehouse psql -U warehouse -W warehouse

update-dags:
	@echo "Restarting Airflow container to update DAGs..."
	docker-compose restart airflow-webserver
	@echo "Airflow container restarted and DAGs updated."