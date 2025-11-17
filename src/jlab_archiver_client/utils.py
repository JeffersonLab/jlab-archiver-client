"""Utility functions for processing data from myquery."""
from typing import Any, Dict, List

import numpy as np
import pandas as pd


def convert_data_to_series(values: List[Any], ts: List[Any], name: str, metadata: Dict[str, Any],
                           enums_as_strings: bool) -> pd.Series:
    """Process the data response from myquery.

    If the data is scalar (datasize == 1), then pandas can automatically determine the type.  When datasize > 1, myquery
    returns an array of strings for each value, and the client must convert them to the appropriate datatype.

    Args:
        values: Array of myquery values to process.
        ts: Array of timestamps associated with the values.
        name: Name of the channel queried.
        metadata: Channel metadata returned by myquery.
        enums_as_strings: Should enums be displayed in their string names?

    Returns:
        A pandas Series with the data converted from myquery.  Vector valued responses are converted to the
        appropriate datatype.  The index is the timestamps of each sample.
    """

    if metadata['datasize'] == 1:
        if metadata["returnCount"] == 0:
            data = pd.Series([], index=ts, name=name, dtype=object)
        else:
            data = pd.Series(values, index=ts, name=name)
    else:
        # myquery returns vector data as an array of strings.  Need to manually convert to desired format
        if metadata['datatype'] in ("DBR_DOUBLE", "DBR_FLOAT"):
            # Cast to float (64-bit is adequate for both)
            data = pd.Series(values, index=ts, name=name)
            data = data.apply(lambda x: np.array(np.array(x), dtype=float))
        elif metadata['datatype'] in ("DBR_SHORT", "DBR_LONG"):
            # Cast to int (64-bit is adequate for both)
            data = pd.Series(values, index=ts, name=name)
            data = data.apply(lambda x: np.array(np.fromstring(x, sep=","), dtype=int))
        elif metadata['datatype'] == "DBR_ENUM" and not enums_as_strings:
            data = pd.Series(values, index=ts, name=name)
            data = data.apply(lambda x: np.array(np.fromstring(x, sep=","), dtype=int))
        else:
            # This will return values as an array of str
            data = pd.Series(values, index=ts, name=name)

    data.index = pd.to_datetime(data.index)

    return data


def convert_data_to_dataframe(samples: Dict[str, Any], metadata: Dict[str, Dict[str,Any]],
                           enums_as_strings: bool) -> pd.DataFrame:
    """Process the data response from myquery if multiple channels are included.

    If the data is scalar (datasize == 1), then pandas can automatically determine the type.  When datasize > 1, myquery
    returns an array of strings for each value, and the client must convert them to the appropriate datatype.

    Args:
        samples: Array of myquery values to process.  Should include "Date" and channel name as keys, and Lists of
                 values as the dict values
        metadata: Channel metadata returned by myquery.  Keyed on channel names
        enums_as_strings: Should enums be displayed as their string names

    Returns:
        A pandas DataFrame with the data converted from myquery.  Vector valued responses are converted to the
        appropriate datatype.  The index is the Date field converted to a DateTimeIndex.
    """
    # Iterate through the channels and convert them if needed.
    for channel in samples.keys():
        if channel == "Date":
            samples[channel] = pd.to_datetime(samples[channel])
            continue

        # Leave scalar valued series alone
        if metadata[channel]['metadata']['datasize'] == 1:
            continue

        # Get the EPICS record type
        rtyp = metadata[channel]['metadata']['datatype']

        def _convert_row(row, dtype) -> np.ndarray:
            """Convert a list of strings to a numpy array of the appropriate type and handle None"""
            if row is None:
                out = None
            else:
                out = np.array(row, dtype=dtype)
            return out

        # Since we only have vector valued channels, we need to convert from the str type that myquery supplies
        if rtyp in ("DBR_DOUBLE", "DBR_FLOAT"):
            # Cast to float (64-bit is adequate for both)
            samples[channel] = list(map(lambda x: _convert_row(x, float), samples[channel]))
        elif rtyp in ("DBR_SHORT", "DBR_LONG"):
            # Cast to int (64-bit is adequate for both)
            samples[channel] = list(map(lambda x: _convert_row(x, int), samples[channel]))
        elif rtyp == "DBR_ENUM" and not enums_as_strings:
            samples[channel] = list(map(lambda x: _convert_row(x, int), samples[channel]))
        else:
            # We will leave them as a list of strings
            pass

    data = pd.DataFrame(samples).set_index("Date", drop=True)

    return data


def json_normalize(obj: Any) -> Any:
    """Prepare data for use by json.dump.

    Pandas won't json encode nicely and more flexible to save numpy numbers directly

    Args:
        obj: Object to be converted.

    Returns:
        Converted object, ready for JSON serialization with json.JSONEncoder.
    """

    if isinstance(obj, pd.Series):
        # Does not support "split".  => {"__type__: "series", idx1: val1, idx2:val2, ...}
        # Recursively normalize to handle nested pandas structures (pd.Timestamp, etc.)
        return {"__type__": "series", **json_normalize(obj.to_dict())}
    if isinstance(obj, pd.DataFrame):
        # split => {"index": [], "columns": [], "data": []}
        # Recursively normalize to handle nested pandas structures (pd.Timestamp, etc.)
        return {"__type__": "dataframe", **json_normalize(obj.to_dict(orient="split"))}
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.integer, np.floating, np.bool_)):
        return obj.item()
    if isinstance(obj, (pd.Timestamp, pd.Timedelta)):
        return str(obj)
    if isinstance(obj, dict):
        # ensure JSON-safe keys and normalized values
        return {str(k): json_normalize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [json_normalize(v) for v in obj]

    return obj
