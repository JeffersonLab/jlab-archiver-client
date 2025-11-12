from typing import Optional, Dict, Any

import requests

from jlab_archiver_client.config import config
from jlab_archiver_client.query import PointQuery

__all__ = ["Point"]


class Point:
    """A class for running calls to myquery's point endpoint.

    This endpoint returns a single channel event.  The user supplies the channel name and a timestamp, and myquery
    return the closest event before (inclusive) the timestamp.

    Options exists to look into the future and exclude
    the given timestamp from search space.  That is helpful in the scenario that you want to know the next event
    before/after an event you already know of.
    """

    def __init__(self, query: PointQuery, url: Optional[str] = None):
        """Construct an instance for running a myquery interval.

        Args:
            query: The query to run
            url: The location of the myquery/interval endpoint. Generated from config if None supplied.
        """
        self.query = query
        self.url = url
        if url is None:
            self.url = f"{config.protocol}://{config.myquery_server}{config.point_path}"

        self.event: Optional[Dict[str, Any]] = None

    def run(self):
        """Run a web-based myquery interval query.  This supports querying only one PV at a time.

        Raises:
            RequestException when a problem making the query has occurred
        """

        opts = self.query.to_web_params()
        r = requests.get(self.url, params=opts)
        print(r.url)

        if r.status_code != 200:
            raise requests.RequestException(f"Error contacting server. status={r.status_code}")

        self.event = r.json()
