import logging
from pipeline_utils import extract, transform, load

logging.basicConfig(filename="./logs/etl_log.txt", format='[%(asctime)s] [%(levelname)s]: %(message)s', level=logging.DEBUG)

STOCK_TICKERS = ["TSLA", "RIVN", "AEHR", "ON", "VFS"]

try:
  raw_stock_data = extract(STOCK_TICKERS, True)
  transformed_stock_data = transform(raw_stock_data, True)
  load(transformed_stock_data, "transformed_stock_data")
  logging.info("Successfully executed ETL Pipeline")  # Log a success message
except Exception as e:
  logging.error(f'Pipeline failed with error: {e}') # Log a failure message