# Base image with Python
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Copy requirements first (for caching)
# build context is the repo root (set in docker-compose.yml)
COPY requirements.txt .

# Upgrade pip and build tools first (important for Airflow/dbt)
RUN pip install --upgrade pip setuptools wheel

# # Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt
# Install Python dependencies with Airflow constraints
# RUN pip install --no-cache-dir -r requirements.txt \
#     -c https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.11.txt

# Expose Jupyter Notebook port
EXPOSE 8888

# Default command to start Jupyter Notebook
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--allow-root", "--no-browser"]
