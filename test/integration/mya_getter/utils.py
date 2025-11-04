import json

import pandas as pd


def load_multi_pv_data(ident: str, base_dir: str):
    """Load test case data for mysampler"""
    exp_data = pd.read_csv(f"{base_dir}/data/myquery_{ident}-data.csv", index_col=0)
    exp_data.index = pd.to_datetime(exp_data.index)
    with open(f"{base_dir}/data/myquery_{ident}-disconnects.json", "r") as f:
        exp_disconnects = json.load(f)

    with open(f"{base_dir}/data/myquery_{ident}-metadata.json", "r") as f:
        exp_metadata = json.load(f)

    return exp_data, exp_disconnects, exp_metadata
