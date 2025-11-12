"""This module is responsible for wrapping the the myquery/mystats end point."""

from typing import Optional, Dict

import pandas as pd
import requests

from jlab_archiver_client.query import MyStatsQuery
from jlab_archiver_client.config import config

__all__ = ["MyStats"]


class MyStats:
    """A class for running a myquery mystats request and holding the results.  Only float PVs supported.

    Statistics are stored in the data field.  This is a DataFrame with a MultiIndex on the start of the bin and the
    metric name.

    Metadata is stored in a dictionary key on each channel.

    The mystats endpoint is intended to provide the value of a set of PVs at regularly spaced time intervals.
    """

    def __init__(self, query: MyStatsQuery, url: Optional[str] = None):
        """Construct an instance for running a mystats query.

        Args:
            query: The query to run
            url: The location of the mystats endpoint.  Generated from config if None supplied.
        """
        self.query = query
        self.url = url
        if url is None:
            self.url = f"{config.protocol}://{config.myquery_server}{config.mystats_path}"

        self.data: Optional[pd.DataFrame] = None
        self.metadata: Optional[Dict[str, object]] = None

    @staticmethod
    def _channel_series(channel_obj):
        """Return a Series indexed by (timestamp, stat) holding the metric values."""
        tuples, vals = [], []

        # Look at the first entry to determine what metrics to include
        metrics = []
        for key in channel_obj["data"][0].keys():
            if key != "begin":
                metrics.append(key)
        metrics = sorted(metrics)

        for rec in channel_obj["data"]:
            ts = pd.to_datetime(rec["begin"])
            for m in metrics:
                tuples.append((ts, m))
                vals.append(rec.get(m))
        idx = pd.MultiIndex.from_tuples(tuples, names=["timestamp", "stat"])
        return pd.Series(vals, index=idx)

    def run(self):
        """Run a web-based mysampler query.

        Results will be stored in the data, disconnects, and metadata fields.

        Raises:
            RequestException when a problem making the query has occurred
        """

        # Make the request
        opts = self.query.to_web_params()
        r = requests.get(self.url, params=opts)

        # Check if we have any errors
        if r.status_code != 200:
            raise requests.RequestException(f"Error contacting server. status={r.status_code}")

        # Single top level key is channels
        channels = r.json()['channels']

        # Build one Series per channel, then concat side-by-side so columns = channels
        series_by_channel = {ch_name: self._channel_series(ch_obj) for ch_name, ch_obj in channels.items()}

        self.data = pd.concat(series_by_channel, axis=1).sort_index()
        self.metadata = {ch_name: ch_obj['metadata'] for ch_name, ch_obj in channels.items()}
