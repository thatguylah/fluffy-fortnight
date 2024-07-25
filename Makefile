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
	$(VENV_DIR)/bin/pip install --upgrade pip
	$(VENV_DIR)/bin/pip install -r requirements.txt
	touch $(VENV_DIR)/bin/activate

$(VENV_DIR):
	$(PYTHON) -m venv $(VENV_DIR)

# Target to activate the virtual environment
activate:
	@echo "Run 'source $(VENV_DIR)/bin/activate' to activate the virtual environment."

# Target to clean up the virtual environment
clean:
	rm -rf $(VENV_DIR)

etl:
	rm -rf data/output/datawarehouse.duckdb
	python scripts/ddl.py
	python scripts/load_to_bronze.py 
	python scripts/transform_to_silver.py
	python scripts/one_time_scraper.py
	python scripts/transform_to_silver_mappings.py
	python scripts/transform_to_gold.py
	python scripts/post_run_indexing.py
	python scripts/clustering.py
	python scripts/load_clustering_results.py
	# streamlit run visualization/dashboard.py
.PHONY: all setup activate clean