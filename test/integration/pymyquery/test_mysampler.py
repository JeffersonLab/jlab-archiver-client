import os
import unittest
import json
from datetime import datetime
from typing import Dict

import pandas as pd

from pymyquery.mysampler import MySampler
from pymyquery.query import MySamplerQuery
from pymyquery.utils import json_normalize


DIR = os.path.dirname(__file__)


class TestMySampler(unittest.TestCase):

    @staticmethod
    def load_mysampler_data(ident: str):
        """Load test case data for mysampler"""
        exp_data = pd.read_csv(f"{DIR}/data/myquery_{ident}-data.csv", index_col=0)
        exp_data.index = pd.to_datetime(exp_data.index)
        with open(f"{DIR}/data/myquery_{ident}-disconnects.json", "r") as f:
            exp_disconnects = json.load(f)

        with open(f"{DIR}/data/myquery_{ident}-metadata.json", "r") as f:
            exp_metadata = json.load(f)

        return exp_data, exp_disconnects, exp_metadata

    @staticmethod
    def save_mysampler_data(ident: str, data: pd.DataFrame, disconnects: Dict[str, pd.DataFrame],
                            metadata: Dict[str, object]):
        """Convenient way to save test case data for mysampler."""
        data.to_csv(f"{DIR}/data/myquery_{ident}-data.csv")

        with open(f"{DIR}/data/myquery_{ident}-disconnects.json", "w") as f:
            # noinspection PyTypeChecker
            json.dump(json_normalize(disconnects), f)
            # json.dump(disconnects, f, cls=myquery.MyQueryEncoder)

        with open(f"{DIR}/data/myquery_{ident}-metadata.json", "w") as f:
            # noinspection PyTypeChecker
            json.dump(json_normalize(metadata), f)

    def check_mysampler_result(self, exp_data, exp_disconnects, exp_metadata, res_data,
                               res_disconnects, res_metadata):
        """Utility function for checking tes results of mysampler"""
        self.assertTrue(exp_data.equals(res_data), f"\nExpected:\n{exp_data}\nResult:\n{res_data}\n")
        self.assertDictEqual(exp_disconnects, json_normalize(res_disconnects),
                                  f"\nExpected:\n{exp_disconnects}\nResult:\n{res_disconnects}\n")
        self.assertDictEqual(exp_metadata, json_normalize(res_metadata),
                                  f"\nExpected:\n{exp_metadata}\nResult:\n{res_metadata}\n")

    def test_get_mysampler_1(self):
        """Test basic query with lots of default values. (No NaN in sample, but in interval)"""

        query = MySamplerQuery(start=datetime.strptime("2018-04-24 12:00:00", "%Y-%m-%d %H:%M:%S"),
                                       interval=600_000,  # 10 minutes
                                       num_samples=10,
                                       pvlist=["channel100", "channel101"],
                                       deployment="docker")

        mysampler = MySampler(query)
        mysampler.run()
        res_data = mysampler.data
        res_disconnects = mysampler.disconnects
        res_metadata = mysampler.metadata

        exp_data, exp_disconnects, exp_metadata = self.load_mysampler_data("mysampler_1")
        self.check_mysampler_result(exp_data, exp_disconnects, exp_metadata, res_data, res_disconnects,
                                    res_metadata)

    def test_get_mysampler_2(self):
        """Test basic query with lots of default values. (No NaN in sample, but in interval)"""

        query = MySamplerQuery(start=datetime.strptime("2018-04-24 00:00:00", "%Y-%m-%d %H:%M:%S"),
                                       interval=3_600_000, # hourly
                                       num_samples=15,
                                       pvlist=["channel100", "channel101"],
                                       deployment="docker")

        mysampler = MySampler(query)
        mysampler.run()
        res_data = mysampler.data
        res_disconnects = mysampler.disconnects
        res_metadata = mysampler.metadata

        exp_data, exp_disconnects, exp_metadata = self.load_mysampler_data("mysampler_2")
        self.check_mysampler_result(exp_data, exp_disconnects, exp_metadata, res_data, res_disconnects,
                               res_metadata)
