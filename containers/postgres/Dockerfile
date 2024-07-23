# Use the official PostgreSQL base image
FROM postgres:16

# Copy the DDL.sql script to the container's initdb directory
COPY DDL.sql /docker-entrypoint-initdb.d/
