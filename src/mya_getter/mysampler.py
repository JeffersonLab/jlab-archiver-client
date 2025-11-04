from typing import Optional, Dict

import pandas as pd
import requests

from mya_getter.query import MySamplerQuery


__all__ = ["MySampler"]


class MySampler:
    """A class for running a myquery mysampler request and holding the results.

    Data from all PVs are stored the data field as a single DataFrame as they
    share a common time index.  Non-update events are stored as None in the
    data field.  This should allow pandas automatic type detection to work
    in the case of non-update events.

    The diconnects field contains a dictionary that is keyed on each PV with
    values that are a Series of only the disconnect events.  The values of this
    Series contain the original text associated with the non-update events.

    Additional metadata from the myquery/mysampler response is contained in a
    dictionary under the metadata field.

    The mysampler endpoint is intended to provide the value of a set of PVs at
    regularly spaced time intervals.
    """

    def __init__(self, query: MySamplerQuery, url: str = "https://epicsweb.jlab.org/myquery/mysampler"):
        """Construct an instance for running a mysampler query.

        Args:
            query: The query to run
            url: The location of the mysampler endpoint
        """
        self.query = query
        self.url = url

        self.data: Optional[pd.DataFrame] = None
        self.disconnects: Optional[Dict[str, pd.Series]] = None
        self.metadata: Optional[Dict[str, object]] = None

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

        # This will hold the information for the data, disconnects, and metadata fields respectively
        samples = {'Date': []}
        disconnects = {}
        metadata = {}

        # Process the response for each channel
        for idx, channel in enumerate(channels.keys()):
            for key in channels[channel].keys():
                if key == "data":
                    v = []
                    dv = []
                    dts = []
                    for sample in channels[channel]['data']:
                        # Grab only one datetime series
                        if idx == 0:
                            samples['Date'].append(sample['d'])

                        # Handle disconnect events
                        if 't' in sample.keys():
                            v.append(None)
                            dts.append(sample['d'])
                            dv.append(sample['t'])
                        else:
                            v.append(sample['v'])
                    samples[channel] = v

                    if len(dts) > 0:
                        disconnects[channel] = pd.Series(dv, index=dts, name=channel)

                else:
                    if channel not in metadata.keys():
                        metadata[channel] = {}
                    metadata[channel][key] = channels[channel][key]

        data = pd.DataFrame(samples)
        data.Date = pd.to_datetime(data.Date)
        data = data.set_index("Date")

        # Update the object with the processed response
        self.data = data
        self.disconnects = disconnects
        self.metadata = metadata
