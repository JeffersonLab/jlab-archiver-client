
from typing import Any, Dict, List

import numpy as np
import pandas as pd


def convert_data_to_pandas(values: List[Any], ts: List[Any], name: str, metadata: Dict[str, Any],
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
    """
    if metadata['datasize'] == 1:
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
        return {"__type__": "series", **obj.to_dict()}
    if isinstance(obj, pd.DataFrame):
        # split => {"index": [], "columns": [], "data": []}
        return {"__type__": "dataframe", **obj.to_dict(orient="split")}
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
