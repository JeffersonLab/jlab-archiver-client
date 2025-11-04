from typing import List
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import concurrent.futures





def do_parallel_queries(func: callable, queries: List[Query], max_workers: int = 8):
    """Call a mya utility wrapper in parallel for each query.  Returns a single DataFrame for all results.

    Args:
        func: A function that wraps one of the MYA utilities, e.g., mySampler.
        queries: A list of queries to be used by func, e.g. MySamplerQuery for mySampler
        max_workers: The maximum number of workers used in the process pool.
    """
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:

        # Submit a bunch of jobs to a pool.
        futures = []
        for i in range(len(queries)):
            futures.append(
                executor.submit(func, queries[i]))
        #  Wait for results.  tqdm gives a progress bar
        for future in tqdm(concurrent.futures.as_completed(futures)):
            results.append(future.result())

        # Concat all of the individual DFs back into one big one.  Add a column that tracks which query a row is from
        mya_df = pd.concat(results, keys=[f"query_{i}" for i in range(len(queries))])
        mya_df = mya_df.reset_index().drop(columns=['level_1'])

    return mya_df
