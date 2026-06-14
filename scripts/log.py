import logging

logging.basicConfig(
    filename='/opt/airflow/logs/pipeline.log',
    level=logging.INFO
)

def log(message):
    logging.info(message)
