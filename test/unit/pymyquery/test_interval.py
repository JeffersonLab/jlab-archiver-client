import os
import unittest
import json
from datetime import datetime
from typing import Dict

import numpy as np
import pandas as pd

from pymyquery.interval import Interval
from pymyquery.query import IntervalQuery
from pymyquery.utils import json_normalize


DIR = os.path.dirname(__file__)


class TestInterval(unittest.TestCase):
    s1 = pd.Series(
        [7.930, 0.000, 6.996, 7.930, 7.755, np.nan, 7.755, np.nan],
        index=pd.to_datetime([
            "2018-04-24 00:00:00",
            "2018-04-24 11:12:51",
            "2018-04-24 11:12:55",
            "2018-04-24 11:12:56",
            "2018-04-24 11:18:18",
            "2018-04-24 12:19:44",
            "2018-04-24 12:32:45",
            "2018-04-25 01:20:45",
        ]),
        name="R123GMES",
        dtype="float64",
    )

    s2 = pd.Series(
        [5.911, 0.0, np.nan, 5.66, np.nan, 5.657, 5.657],
        index=pd.to_datetime([
            "2018-04-24 00:00:00",
            "2018-04-24 06:25:01",
            "2018-04-24 06:25:05",
            "2018-04-24 11:18:19",
            "2018-04-24 12:19:44",
            "2018-04-24 12:31:11",
            "2018-04-25 01:20:45"]),
        name="R121GMES",
        dtype="float64",
    )

    df_s1_s2 = pd.DataFrame(
        {
            "R123GMES": [7.930, 7.930, 7.930, 0.000, 6.996, 7.930, 7.755, 7.755, np.nan, np.nan, 7.755, np.nan],
            "R121GMES": [5.911, 0.000, np.nan, np.nan, np.nan, np.nan, np.nan, 5.660, np.nan, 5.657, 5.657, 5.657],
        },
        index=pd.to_datetime([
            "2018-04-24 00:00:00",
            "2018-04-24 06:25:01",
            "2018-04-24 06:25:05",
            "2018-04-24 11:12:51",
            "2018-04-24 11:12:55",
            "2018-04-24 11:12:56",
            "2018-04-24 11:18:18",
            "2018-04-24 11:18:19",
            "2018-04-24 12:19:44",
            "2018-04-24 12:31:11",
            "2018-04-24 12:32:45",
            "2018-04-25 01:20:45",
        ]),
    )

    def test__combine_series(self):
        """Test if internal logic for combining series works."""
        exp = self.df_s1_s2
        result = Interval._combine_series([self.s1, self.s2])
        self.assertTrue(exp.equals(result), f"\nExp:\n{exp}\nResult:\n{result}\n")
