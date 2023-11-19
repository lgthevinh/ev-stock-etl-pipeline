import sys

sys.path.append('F:\Code\Personal Project\ev-stock-etl-pipeline\scripts')

import unittest
import pandas as pd

from pipeline_utils import transform

class TestTransform(unittest.TestCase):

  def setUp(self) -> None:
    self.raw_data = pd.read_csv("./data/raw/raw_stock_data.csv")
  
  def test_transform_data(self):
    self.cleanned_data = transform(self.raw_data)
    
    self.assertIsNotNone(self.cleanned_data)
    self.assertTrue(isinstance(self.cleanned_data, pd.DataFrame))
    self.assertTrue(len(self.cleanned_data.columns) <= len(self.raw_data.columns))

if __name__ == '__main__':
  unittest.main()