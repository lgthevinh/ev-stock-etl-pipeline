from sqlalchemy import create_engine

connection_string = 'postgresql://lgthevinh:4bokUp9IiNVG@ep-solitary-haze-93016686.ap-southeast-1.aws.neon.tech/data_engineer?sslmode=require'

def db_engine():
  return create_engine(connection_string)