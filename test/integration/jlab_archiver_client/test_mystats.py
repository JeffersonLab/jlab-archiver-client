import unittest
import json
from datetime import datetime
from typing import Tuple, Any, Dict

import numpy as np
import pandas as pd
from jlab_archiver_client import MyStatsQuery, MyStats


class TestMyStats(unittest.TestCase):

    @staticmethod
    def construct_expected() -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Construct expected data and metadata for MyStats query integration tests.

        This helper method creates a reference DataFrame and metadata dictionary that represent
        the expected output from a MyStats query for two channels (channel100 and channel101)
        over a 6-day period from 2024-04-24 to 2024-04-29.

        The method constructs a pandas DataFrame with a MultiIndex structure where:
            - Level 0 (timestamp): Daily timestamps from 2024-04-24 to 2024-04-29 (6 days)
            - Level 1 (stat): Nine statistical measures for each timestamp

        Statistical measures included (in order):
            - duration: Time span of the bin in seconds (86400 = 24 hours)
            - eventCount: Number of events recorded in the bin
            - integration: Integrated value over the time period
            - max: Maximum value observed
            - mean: Average value
            - min: Minimum value observed
            - rms: Root mean square value
            - stdev: Standard deviation
            - updateCount: Number of updates in the bin

        Returns:
            tuple[pd.DataFrame, dict]: A tuple containing:
                - data (pd.DataFrame): A DataFrame with MultiIndex (timestamp, stat) and columns
                  for each channel. Each channel contains the same statistical pattern repeated
                  for each day in the date range.
                - metadata (dict): A dictionary mapping channel names to their metadata, including
                  name, datatype (DBR_DOUBLE), datasize, datahost (mya), ioc, and active status.

        Note:
            The statistical values are constant across all days for each channel, simulating
            a stable signal with known characteristics. This is useful for validating that
            the MyStats query correctly retrieves and formats data.

        Examples:
            >>> data, metadata = TestMyStats.construct_expected()
            >>> data.loc[("2024-04-24", "mean"), "channel100"]
            5.658
            >>> metadata["channel100"]["datatype"]
            'DBR_DOUBLE'
        """
        # Define the index levels
        dates = pd.date_range("2024-04-24", "2024-04-29", freq="D")
        stats = [
            "duration", "eventCount", "integration", "max", "mean",
            "min", "rms", "stdev", "updateCount"
        ]

        # Create a MultiIndex from their Cartesian product
        index = pd.MultiIndex.from_product([dates, stats], names=["timestamp", "stat"])

        # Fill in the repeated numeric pattern
        data = {
            "channel100": np.tile([
                86400.000000, 2.000000, 488851.199341, 5.658000, 5.658000,
                5.658000, 5.658000, 0.000000, 1.000000
            ], len(dates)),
            "channel101": np.tile([
                86400.000000, 2.000000, 669859.181213, 7.753000, 7.753000,
                7.753000, 7.753000, 0.000000, 1.000000
            ], len(dates)),
        }

        data = pd.DataFrame(data, index=index)

        metadata = {'channel100': {'name': 'channel100', 'datatype': 'DBR_DOUBLE', 'datasize': 1, 'datahost': 'mya',
                                   'ioc': None, 'active': True},
                    'channel101': {'name': 'channel101', 'datatype': 'DBR_DOUBLE', 'datasize': 1, 'datahost': 'mya',
                                   'ioc': None, 'active': True}}

        return data , metadata

    def test_get_mystats(self):
        """Test basic usage"""
        #  http://localhost:8080/myquery/mystats?c=channel100,channel101&b=2024-04-24&e=2024-04-30&n=6&m=docker&f=&v=
        exp_data, exp_metadata = self.construct_expected()

        mystats = MyStats(MyStatsQuery(pvlist=["channel100", "channel101"],
                                       start=datetime.strptime("2024-04-24", "%Y-%m-%d"),
                                       end=datetime.strptime("2024-04-30", "%Y-%m-%d"),
                                       num_bins=6,
                                       deployment="docker")
                          )
        mystats.run()

        res_data = mystats.data
        res_metadata = mystats.metadata

        pd.testing.assert_frame_equal(exp_data, res_data)
        self.assertDictEqual(exp_metadata, res_metadata)
