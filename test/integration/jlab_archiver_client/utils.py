from typing import Any

import numpy as np
import pandas as pd


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
