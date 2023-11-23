import pandas as pd
import requests
import logging
from db_engine import db_engine

logging.basicConfig(filename="./logs/etl_log.txt", format='[%(asctime)s] [%(levelname)s]: %(message)s', level=logging.DEBUG)

# Pre-define all constants needed
URL = "https://real-time-finance-data.p.rapidapi.com/stock-quote"       # API get from RapidAPI (https://rapidapi.com/letscrape-6bRBa3QguO5/api/real-time-finance-data/)
headers = {
	"X-RapidAPI-Key": "e2ffeb54aemsh5968fd3b770a4c9p164705jsn6e9fb2167119",
	"X-RapidAPI-Host": "real-time-finance-data.p.rapidapi.com"
}

# Create database engine
db_engine = db_engine()

def query_string(stock):
  return {"symbol": stock, "language":"en"}

def classify_diff(row):
  diff = row["prev_close"] - row["open_price"]
  if diff < 0:
    return "Decrease"
  elif diff == 0:
    return "No difference"
  return "Increase"

def classify_change(row):
  diff = row["prev_close"] - row["open_price"]
  return "%.2f" %diff

def extract(stocks, breakpoint = False):
  parsed_data = []
  try:
    for stock in stocks:
      response = requests.get(URL, headers=headers, params=query_string(stock))
      columns = []
      columns.extend(list(response.json()["data"].values()))
      parsed_data.append(columns)
    raw_data = pd.DataFrame(parsed_data)
    raw_data.columns = ["symbol", "stock_name", "type", "price", "open_price", "high", "low", "volume", "prev_close", "change", "change_percent", "pre_post_market", "pre_post_change", "pre_post_change_percent", "last_update"]
    raw_data.set_index("last_update")
    logging.info(f'Sucessfully extract data from {URL}')
    if breakpoint: # breakpoint is used to save the raw data to a csv file for testing purpose, ONLY USE IN TEST MODE
      try:
        raw_data.to_csv("./data/raw/raw_stock_data.csv", index=False, header=False, mode="a")
        logging.info(f'Successfully save raw data breakpoint in "/data/raw/raw_stock_data.csv"')
      except:
        logging.warning(f'Failed to save raw data breakpoint')
    return raw_data
  except Exception as e:
    logging.error(f'Failed to extract data from {URL} with error: {e}')
    raise e

def transform(raw_data, breakpoint = False):
  try:
    transformed_data = raw_data.loc[:, ["symbol", "stock_name", "price", "open_price", "prev_close", "change", "change_percent", "last_update"]]
    transformed_data["premarket_change"] = transformed_data.apply(classify_change, axis=1)
    transformed_data["premarket_diff"] = transformed_data.apply(classify_diff, axis=1)
    transformed_data["last_update"] = pd.to_datetime(transformed_data["last_update"]).dt.strftime("%Y-%m-%d")
    logging.info("Successfully transform data")
    if breakpoint: # breakpoint is used to save the transformed data to a csv file for testing purpose, ONLY USE IN TEST MODE
      try:
        transformed_data.to_csv("./data/processed/transformed_stock_data.csv", index=False, header=False, mode="a")
        logging.info(f'Successfully save raw data breakpoint in "/data/processed/transformed_stock_data.csv"')
      except:
        logging.warning(f'Failed to save transformed data breakpoint')
    return transformed_data
  except Exception as e:
    logging.error(f'Failed to transformed data with error: {e}')
    raise e

def load(cleaned_data: pd.DataFrame, table: str, test_mode = False):
  try:
    to_validate = pd.read_sql(f'SELECT * FROM {table}', db_engine)
    to_validate["last_update"] = pd.to_datetime(to_validate["last_update"])
    cleaned_data["last_update"] = pd.to_datetime(cleaned_data["last_update"])
    if cleaned_data["last_update"].isin(to_validate["last_update"]).any(): # Check if there is any duplicated data --> if yes, do not load
      logging.info(f'No new data to load to {table} table')
    else:
      cleaned_data.to_sql(table, db_engine, if_exists="append", index=False)
      logging.info(f'Successfully load data to {table} table')
    if test_mode:
      return True
  except Exception as e:
    logging.error(f'Failed to load data with error: {e}')
    raise e
