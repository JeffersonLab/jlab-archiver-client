import warnings
from datetime import datetime
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

from pymyquery import IntervalQuery, Interval


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


def collapse_overlapping_intervals(df: pd.DataFrame, start: str, end: str, aggs: Dict[str, str] = None) -> pd.DataFrame:
    """This finds the overlapping intervals in a DataFrame and collapses them into a single interval.

    Args:
        df: A DataFrame containing at least two columns, a start and an end
        start: The name of the start column
        end: The name of the end column
        aggs: A dictionary of aggregation functions that should be applied to specific columns.  Code uses {start: min,
              end: max} to generate the non-overlapping interval unless overridden via aggs.
    """

    if aggs is None:
        aggs = {}

    tmp = df.sort_values(start)
    tmp['interval_id'] = (tmp[start] > tmp[end].shift().cummax()).cumsum()

    return tmp.groupby(['interval_id']).agg({start: 'min', end: 'max', **aggs})


def interval_overlap_any(int1: pd.Series, int2: pd.Series) -> List[bool]:
    """Checks if each interval in first series overlaps with any interval in the seconds series.

    Useful for seeing if an event occurred (flag was raise, etc.) during some other event.

    Args:
        int1: A series of dtype pd.Interval
        int2: A series of dtype pd.Interval
    """
    overlaps = [False] * len(int1)
    rf_overlaps = [False] * len(int2)
    start_j = 0
    for i in range(len(int1)):
        for j in range(start_j, len(int2)):
            if int1[i].overlaps(int2[j]):
                overlaps[i] = True
                rf_overlaps[j] = True
                start_j = j
                break
            if int2[j].left > int1[i].right:
                break

    return overlaps


def get_down_state_intervals(df, on_state=1, off_state=0) -> Tuple[List[datetime], List[datetime]]:
    """Turn a DataFrame containing rows for each state change into a DataFrame containing the start/end of down times.

    Args:
        df: A Dataframe containing a Datetime columns and a 'state' column in position 1.
        on_state: The value that represents the system is in an "on" state
        off_state: The value that represents the system is in an "off" state

    Returns:
         A two-tuple with a lists of start times and end times.  Each start[i], end[i] is a single down state interval.
    """
    starts = []
    ends = []

    s = None
    e = None
    for i in range(len(df)):
        # If MYA lost track of the PV, just skip the fault we might be in.
        if pd.isna(df.iloc[i, 1]):
            s = None
            e = None
        # Only start intervals when a hall stops receiving beam
        elif s is None and e is None:
            if df.iloc[i, 1] == on_state:
                pass
            else:
                s = df.Date[i]
        elif df.iloc[i, 1] == off_state:
            if s is None:
                s = df.Date[i]
                e = None
            else:
                warnings.warn(f"Found trip start with previous start still in memory.  Date: {df.iloc[i, 0]}, "
                              f"{df.columns[1]}: {df.iloc[i, 1]}")
                s = None
                e = None
        elif df.iloc[i, 1] == on_state:
            if e is None:
                e = df.Date[i]
                starts.append(s)
                ends.append(e)
                s = None
                e = None
            else:
                warnings.warn(f"Found trip end with previous end still in memory.  {df.iloc[i, :].to_string()}")
                s = None
                e = None

    return starts, ends


def remove_repeat_values(df, values_col):
    """This removes rows that contain the same value as the previous row in chronological order.

    Typically this is useful when you have collapsed the value space of a PV down.  For example from a full numeric
    range down to some simplification like {0 if PV==0, 1 if PV > 0}.  In this case you can end up with lots of 1's in
    a row that can make later processing difficult.

    Note: This changes the order of rows to be sorted by the 'Date' column
    """

    # Sort these rows chronologically.  Some values have the same date/time so make sure to keep the index order in case
    # of a tie.
    df.rename_axis('idx').sort_values(by=['Date', 'idx'], inplace=True)

    # Iterate through identifying rows that match previous row values to drop
    rows_to_drop = []
    prev = None
    for i in range(len(df)):
        curr = df[values_col][i]
        if prev is None:
            prev = curr
            continue

        if prev == curr:
            rows_to_drop.append(i)

        prev = curr

    # Drop the rows and return
    return df.drop(rows_to_drop).reset_index(drop=True)


def get_combined_down_state_intervals(pvs: List[str], begin: datetime, end: datetime, on_state: int = 1,
                                      off_state: int = 0, max_duration: Optional[float] = None) -> pd.DataFrame:
    """Generates a DataFrame of time intervals that represent the contiguous periods off time where a PV was 'down'.

    Each PV is queried to generate the down state time intervals individually.  Only individual PV intervals of duration
    less that 'max_duration' are kept.  Then overlapping intervals across PVs are combined to produce a single row
    with the earliest start time and latest end time for those overlapping down state intervals.

    Args:
        pvs: A list of PV names
        begin: The start of the myData query
        end: The end of the myData query
        on_state: The value that is considered to be the on or up state
        off_state: The value that is considered to be the off or down state
        max_duration: The longest an individual PV trip interval can be, in seconds, without being excluded.

    Returns:
        A DataFrame of rows with start, end, and duration columns.  One row per down time interval
    """
    pv_trips = []
    for pv in pvs:
        interval = Interval(IntervalQuery(begin=begin, end=end, pvlist=[pv]))
        tmp_df = interval.data
        tmp_df.iloc[:, 1] = pd.to_numeric(tmp_df.iloc[:, 1], errors='coerce')
        starts, ends = get_down_state_intervals(tmp_df, on_state=on_state, off_state=off_state)
        tmp_df = pd.DataFrame({'pv': [pv] * len(starts), 'start': starts, 'end': ends})
        pv_trips.append(tmp_df)

    # Get one DataFrame of beam stoppages, then keep only trips (those <= 5 minutes)
    df = pd.concat(pv_trips).reset_index(drop=True)
    df['duration'] = df.apply(lambda x: (x.end - x.start).total_seconds(), axis=1)
    if max_duration is not None:
        df = df[df.duration <= max_duration]

    # Combine the trips seen by all the different halls into one set of non-overlapping downtimes
    df = collapse_overlapping_intervals(df, start='start', end='end')

    return df


def get_rf_trip_intervals(begin: datetime, end: datetime):
    """This queries data on the NL RF trip count and beam presence in the halls to generate RF-involved trip intervals.

    This is currently left in the application code as an example of how the functionality of the of trips package could
    be used.
    """
    # fsd_master_pv = "ISD0I011G"
    rf_trip_pv = 'FSDTRIPRFNLCNT'
    # hall_pvs = ['HLA:bta_bm_present', 'HLB:bta_bm_present', 'HLC:bta_bm_present', 'HLD:bta_bm_present']

    # The BPMs have a flag for beam presence.  Beam Not Sensed Flag (BNSF).  0 => present, 1 => not present
    beam_present_pv = 'IPM1A01.BNSF'

    # This includes daylight savings change over.  Just drop those from consideration to make this easier.
    overlap_start = datetime.strptime("2021-11-07 01:00:00", "%Y-%m-%d %H:%M:%S")
    overlap_end = datetime.strptime("2021-11-07 02:00:00", "%Y-%m-%d %H:%M:%S")

    # Find the trips as seen by each hall
    # hall_trip_df = get_combined_down_state_intervals(pvs=hall_pvs, begin=begin, end=end, on_state=1, off_state=0,
    #                                                  max_duration=300)

    # Find the start and end times of beam trips
    interval_bp = Interval(IntervalQuery(begin=begin, end=end, pvlist=[beam_present_pv]))
    beam_present_df = interval_bp.data
    beam_present_df.iloc[:, 1] = pd.to_numeric(beam_present_df.iloc[:, 1], errors='coerce')
    beam_present_df = beam_present_df[ (beam_present_df.Date > overlap_end) | (beam_present_df.Date < overlap_start)]
    beam_present_df = beam_present_df.reset_index(drop=True)

    beam_starts, beam_ends = get_down_state_intervals(beam_present_df, on_state=0, off_state=2)
    beam_trip_df = pd.DataFrame({'pv': [beam_present_pv] * len(beam_starts), 'start': beam_starts, 'end': beam_ends})
    # Exclude scenarios where the beam stayed off for more than an 5 minutes.  Those aren't "trips"
    beam_trip_df = beam_trip_df[beam_trip_df.apply(lambda x: (x.end - x.start).total_seconds(), axis=1) < 300]
    beam_trip_df = beam_trip_df.reset_index(drop=True)

    # This is a count of the number of RF trips in recent history.  I'm a little hazy on the details.
    # We just want to know if an RF trip happened somewhere in CEBAF that caused beam to trip off.
    interval_rf = Interval(IntervalQuery(begin=begin, end=end, pvlist=[rf_trip_pv]))
    rf_trip_df = interval_rf.data
    # rf_trip_df = myData(MyDataQuery(begin=begin, end=end, pvlist=[rf_trip_pv]))
    rf_trip_df.iloc[:, 1] = pd.to_numeric(rf_trip_df.iloc[:, 1], errors='coerce')
    rf_trip_df.loc[rf_trip_df[rf_trip_pv] > 0, rf_trip_pv] = 1
    rf_trip_df = rf_trip_df[(rf_trip_df.Date > overlap_end) | (rf_trip_df.Date < overlap_start)].reset_index(drop=True)

    rf_trip_df = remove_repeat_values(rf_trip_df, values_col=rf_trip_pv)
    starts, ends = get_down_state_intervals(rf_trip_df, on_state=0, off_state=1)
    rf_trip_df = pd.DataFrame({'pv': [rf_trip_pv] * len(starts), 'start': starts, 'end': ends})

    # This tells us which trips as seen by the halls were caused by RF
    beam_trip_intervals = beam_trip_df.apply(lambda x: pd.Interval(x.start, x.end, closed='left'), axis=1)
    # hall_intervals = hall_trip_df.apply(lambda x: pd.Interval(x.start, x.end, closed='left'), axis=1)
    rf_intervals = rf_trip_df.apply(lambda x: pd.Interval(x.start, x.end, closed='left'), axis=1)
    rf_caused = interval_overlap_any(beam_trip_intervals, rf_intervals)

    return beam_trip_df[rf_caused].reset_index(drop=True)


def get_mya_samples_from_trips(trip_df: pd.DataFrame, data_file: str = None, max_workers: Optional[int] = None):
    """Find the RF trip intervals and get PV samples from MYA during those times.

    This is currently left in the application code as an example of how the functionality of the of trips package could
    be used.
    """

    # Limit PVs to only the North linac this time.
    pvs = ['IBC0R08CRCUR1']
    for z in "23456789ABCDEFGHIJKLMNOP":
        for c in '12345678':
            pvs.append(f"R1{z}{c}GMES")
    for z in ['05', '06', '07', '08', '22', '23', '24', '25', '26', '27']:
        for r in 'gn':
            pvs.append(f"INX1L{z}_{r}DsRt")

    queries = []
    for i in range(len(trip_df)):
        # We want one sample per second
        num_samples = int((trip_df.end[i] - trip_df.start[i]).total_seconds() + 1)
        queries.append(MySamplerQuery(start=trip_df.start[i], interval='1s', num_samples=num_samples, pvlist=pvs))

    if max_workers is not None:
        data_df = do_parallel_queries(func=mySampler, queries=queries, max_workers=max_workers)
    else:
        data_df = do_parallel_queries(func=mySampler, queries=queries)

    if data_file is not None:
        data_df.to_csv(data_file)

    return data_df


def get_trip_subsets(trip_df: pd.DataFrame, rf_zones: List[str] = ('R1M', 'R1N', 'R1O', 'R1P'),
                     ndx_zones: List[str] = ('1L22', '1L23', '1L24', '1L25', '1L26', '1L27'),
                     meta_cols: List[str] = ('level_0', 'Date', 'IBC0R08CRCUR1'), trip_col: str = 'level_0',
                     trip_threshold: float = 0.1):
    """Return a subset of data where trips happened in the specifed zones."""
    gmes_cols = [f"{z}{c}GMES" for z in rf_zones for c in range(1, 9)]
    ndx_cols = [f"INX{z}_{r}DsRt" for z in ndx_zones for r in 'gn']

    has_trip = trip_df.groupby([trip_col])[gmes_cols].apply(lambda x: x.max() - x.min() > trip_threshold).any(axis=1)
    subset_df = trip_df.copy().set_index(trip_col, drop=False).loc[has_trip, :].reset_index(drop=True)

    return subset_df
