import concurrent
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, List, Tuple

import numpy as np
import pandas as pd
import requests

from pymyquery import utils
from pymyquery.config import config
from pymyquery.exceptions import MyqueryException
from pymyquery.query import IntervalQuery

__all__ = ["Interval"]

class Interval:
    """A class for running calls to myquery's interval endpoint.

    Values of the PV updates are stored as a pandas Series object in the data
    field.  Non-update events are stored as None to allow better automatic type
    detection by pandas.  Diconnect events are stored as a pandas Series in the
    disconnects field.  Other response metadata is available in the metadata
    field.

    The interval endpoint is intended for retrieving the mya events over the
    requested time interval.
    """

    def __init__(self, query: IntervalQuery, url: Optional[str] = None):
        """Construct an instance for running a myquery interval.

        Args:
            query: The query to run
            url: The location of the myquery/interval endpoint. Generated from config if None supplied.
        """
        self.query = query
        self.url = url
        if url is None:
            self.url = f"{config.protocol}://{config.myquery_server}{config.interval_path}"

        self.data: Optional[pd.Series] = None
        self.disconnects: Optional[pd.Series] = None
        self.metadata: Optional[Dict[str, object]] = None

    def run(self):
        """Run a web-based myquery interval query.  This supports querying only one PV at a time.

        Raises:
            RequestException when a problem making the query has occurred
        """

        opts = self.query.to_web_params()
        r = requests.get(self.url, params=opts)

        if r.status_code != 200:
            raise requests.RequestException(f"Error contacting server. status={r.status_code}")

        content = r.json()
        values = []
        ts = []
        disconnect_ts = []
        disconnect_values = []
        for item in content['data']:
            if 'x' in item:
                disconnect_values.append(item['t'])
                disconnect_ts.append(item['d'])
                ts.append(item['d'])
                values.append(None)
            else:
                ts.append(item['d'])
                values.append(item['v'])

        # Default value for empty series is in flux.  Future will have dtype of object.  This skips a deprecation warning.
        if len(disconnect_values) == 0:
            disconnects = pd.Series(disconnect_values, index=disconnect_ts, name=self.query.channel, dtype=object)
        else:
            disconnects = pd.Series(disconnect_values, index=disconnect_ts, name=self.query.channel)

        metadata = {}
        for key, value in content.items():
            if key != "data":
                metadata[key] = value

        self.data = utils.convert_data_to_series(values, ts, self.query.channel, metadata, self.query.enums_as_strings)
        self.disconnects = disconnects
        self.metadata = metadata

    @staticmethod
    def create_queries() -> List[IntervalQuery]:
        """Create a list of IntervalQueries, one per PV, with otherwise identical parameters.

        See IntervalQuery for required parameters.
        """

    @staticmethod
    def run_parallel(pvlist: List[str], max_workers: int = 4, **kwargs) -> Tuple[
        pd.DataFrame, Dict[str, pd.Series], dict]:
        """Run multiple IntervalQueries in parallel.  The web endpoint does not support multiple PVs in a single query.

        All queries will have the same options other than channel, which is pulled from pvlist.  prior_point is forced
        to True to ensure that we can intelligently fill NaN values from merging the disparate time stamps.

        Args:
            pvlist: A list of PVs to queries
            max_workers: The maximum number of concurrent queries to run in parallel.

        Returns:
            A Pandas DataFrame of the combined PVs, a dictionry of per-channel disconnect series (keyed on channels),
            and a dictionary of per-channel metadata (keyed on channel)
        """
        if "channel" in kwargs.keys():
            del kwargs["channel"]
        kwargs["prior_point"] = True

        queries = []
        for pv in pvlist:
            queries.append(IntervalQuery(pv, **kwargs))

        out = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for query in queries:
                out[query.channel] = Interval(query)
                futures.append(executor.submit(out[query.channel].run))

            # The futures won't hold results, only the status of the jobs.  Look at out for future.
            done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)

        if len(not_done) > 0:
            raise MyqueryException("Some PV queries did not complete.")

        data = Interval._combine_series([out[channel].data for channel in out.keys()])
        disconnects = {channel: out[channel].disconnects for channel in out.keys()}
        metadata = {channel: out[channel].metadata for channel in out.keys()}

        return data, disconnects, metadata

    @staticmethod
    def _combine_series(series: List[pd.Series]) -> pd.DataFrame:
        """Combine multiple series of PV history into a single DataFrame with shared DateTime Index.

        The series are concat'ed together and sorted so that rows appear in chronological order.  Missing values are
        forward filled, but NaNs in the original data are preserved and forward filled as well.  The names of the
        Series are used as column names in the resulting DataFrame.

        Note: Series are assumed to have a DateTime index.

        Args:
            series: A list of Series objects to combine

        Return:
            A DataFrame of the combined Series objects.
        """
        df = pd.concat(series, axis=1).sort_index().ffill()

        for s in series:
            # Find where there is non-update values (None/NaN)
            nan_mask = s.isnull()

            # Add back any missing NaNs from the original data
            if sum(nan_mask) > 0:
                # This identifies all the timestamps following an NaN.  If the last value is NaN, then the following
                # timestamp will be pd.NaT (not a time)
                next_ts = s.index.to_series().shift(-1)

                # For each row, check if it was originally an NaN, then add back in the NaNs.
                for idx, is_true in nan_mask.items():
                    if is_true:
                        # idx is the last row so we go to the end
                        if next_ts[idx] is pd.NaT:
                            df.loc[df.index >= idx, s.name] = np.nan
                        else:
                            # Fill in any value between "here" and the next "real" update.
                            df.loc[(df.index >= idx) & (df.index < next_ts[idx]), s.name] = np.nan

        return df
