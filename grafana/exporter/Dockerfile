FROM python:3.12-bookworm

RUN apt-get update && \
    apt-get install -y curl gnupg ca-certificates unixodbc unixodbc-dev && \
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list \
    | tee /etc/apt/sources.list.d/mssql-release.list > /dev/null && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    pip install --no-cache-dir pyodbc prometheus-client && \
    rm -rf /var/lib/apt/lists/*

CMD ["python", "/exporter/synapse_exporter.py"]