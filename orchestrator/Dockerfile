# Use an official Python runtime as a parent image
FROM python:3.12.4-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file and install dependencies
COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy all files and directories from the root of the host system to the Docker container
# COPY . .

# Copy additional YAML configuration files
# COPY .user.yml ./user.yml
# COPY dbt_project.yml ./dbt_project.yml
# COPY profiles.yml ./profiles.yml

# Expose the port for Dagster
EXPOSE 3000

# Set the working directory back to /app/orchestrator for running the command
WORKDIR /app/orchestrator

# Run the Dagster application with the specified environment variable
# CMD ["sh", "-c", "DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1 dagster dev"]
CMD ["sh", "-c", "DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1 dagit -h 0.0.0.0 -p 3000"]
