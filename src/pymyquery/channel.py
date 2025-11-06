from typing import Optional, List, Any, Dict

import requests

from pymyquery.config import config
from pymyquery.query import ChannelQuery


__all__ = ["Channel"]


class Channel:
    """A class for running calls to myquery's channel endpoint.

    This class allows for the user to lookup channels in the archive by name using SQL patterns
    """

    def __init__(self, query: ChannelQuery, url: Optional[str] = None):
        """Construct an instance for running a myquery channel call.

        Args:
            query: The query to run
            url: The location of the myquery/interval endpoint. Generated from config if None supplied.
        """
        self.query = query
        self.url = url
        if url is None:
            self.url = f"{config.protocol}://{config.myquery_server}{config.channel_path}"

        self.matches: Optional[List[Dict:str, Any]] = None

    def run(self):
        """Run a web-based myquery channel query."""

        opts = self.query.to_web_params()
        r = requests.get(self.url, params=opts)
        print(r.url)

        if r.status_code != 200:
            raise requests.RequestException(f"Error contacting server. status={r.status_code}")

        content = r.json()

        self.matches = content
