import sys

sys.path.append('F:\Code\Personal Project\ev-stock-etl-pipeline\scripts')

import unittest
import pandas as pd

from pipeline_utils import load

class TestLoad(unittest.TestCase):

  def setUp(self):
    self.transformed_data = pd.read_csv("./data/processed/transformed_stock_data.csv")

  def test_load_data(self):
    result = load(self.transformed_data, "test_data", True)

    self.assertTrue(result)

if __name__ == '__main__':
  unittest.main()