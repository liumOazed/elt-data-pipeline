FROM python:3.8-slim

# Install prerequisites
RUN apt-get update && apt-get install -y \
    curl \
    gnupg2 \
    lsb-release \
    && rm -rf /var/lib/apt/lists/*

# Add PostgreSQL Apt repository
RUN echo "deb [signed-by=/usr/share/keyrings/pgdg.gpg] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/pgdg.gpg \
    && apt-get update \
    && apt-get install -y postgresql-client-17 cron\
    && rm -rf /var/lib/apt/lists/*

COPY start.sh /app/start.sh    
COPY elt_script.py .

WORKDIR /app

RUN echo "0 3 * * * python /app/elt_script.py" | crontab -
CMD ["python", "elt_script.py"]