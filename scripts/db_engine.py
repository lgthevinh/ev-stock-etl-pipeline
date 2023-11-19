from sqlalchemy import URL, create_engine

connection_string = URL.create(
  'postgresql',
  username='lgthevinh', 
  password='4bokUp9IiNVG', 
  host='ep-solitary-haze-93016686.ap-southeast-1.aws.neon.tech', 
  database='data_engineer', 
)

def db_engine():
  return create_engine(connection_string)