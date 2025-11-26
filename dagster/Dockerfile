FROM python:3.11-slim

RUN pip install dagster dagster-webserver dagster-postgres dagster-dbt

WORKDIR /opt/dagster/app

EXPOSE 3000

CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]