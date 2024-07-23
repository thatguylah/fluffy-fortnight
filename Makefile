# Define variables
VENV_DIR = venv
PYTHON = python3
PIP = $(VENV_DIR)/bin/pip

# Define the list of packages to install
REQUIREMENTS = pandas openpyxl lxml autoviz
# Default target
all: setup

# Target to set up the virtual environment and install packages
setup: $(VENV_DIR)/bin/activate

$(VENV_DIR)/bin/activate: $(VENV_DIR)
	$(PYTHON) -m venv $(VENV_DIR)
	$(PIP) install --upgrade pip
	$(PIP) install $(REQUIREMENTS)
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
