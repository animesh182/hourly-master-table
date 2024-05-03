# db_utils.py
import psycopg2
import psycopg2.extras
import logging
from PredictionFunction.utils.params import params
from datetime import datetime


def execute_query(query, query_name):
    today = datetime.now().replace(hour=23, minute=0, second=0, microsecond=0)
    start_of_month = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # Fixed start date
    # start_date = datetime.strptime("2021-01-01", "%Y-%m-%d").replace(
    #     hour=0, minute=0, second=0, microsecond=0
    # )
    # Current date, adjusted to the end of the day
    # today = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)

    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                cur.execute(query, [start_of_month, today])
                conn.commit()
            logging.info(f"Successfully executed query from {query_name}")
    except Exception as e:
        logging.info(f"Error while executing {query_name}: {e}")
