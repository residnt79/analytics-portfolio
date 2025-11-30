FROM python:3.11-slim

RUN pip install dbt-core dbt-postgres

WORKDIR /usr/app/dbt/analytics

CMD ["tail", "-f", "/dev/null"]