# Use the official Airflow base image
FROM apache/airflow:2.10.5

# Set the working directory
WORKDIR /opt/airflow

# Install additional Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Set the default entrypoint
ENTRYPOINT ["/entrypoint"]
CMD ["webserver"]
