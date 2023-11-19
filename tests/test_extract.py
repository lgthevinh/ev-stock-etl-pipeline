import sys

sys.path.append('F:\Code\Personal Project\ev-stock-etl-pipeline\scripts')

import unittest
import pandas as pd

from pipeline_utils import extract

class TestExtract(unittest.TestCase):
  
  def test_extract_data(self):
    self.raw_data = extract(["VFS"])

    self.assertIsInstance(self.raw_data, pd.DataFrame)

if __name__ == '__main__':
  unittest.main()