import argparse
import os
import json
from datetime import datetime, timedelta
from typing import List
from mya import do_parallel_queries
from mya.mysampler import MySamplerQuery, mySampler
from mya.mydata import MyDataQuery, myData

app_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
app_name = os.path.basename(app_root)


def process_input_file(filename: str):
    """Process a config file that specifies the data to collect.

    Args:
        filename:  The name of the file parse
    """
    queries = []
    valid_subs = ['mysampler', 'mydata']
    with open(filename, mode="r") as f:
        jsondata = ''.join(line for line in f if not line.strip().startswith('#'))
        config = json.loads(jsondata)
        cmd = config['subcommand']
        if cmd not in valid_subs:
            raise RuntimeError(f"Unrecognized subcommand.  Valid subcommands = {valid_subs}")

        for query in config['queries']:
            pvs = query['pvlist']
            for period in query['periods']:
                if cmd == "mysampler":
                    queries.append(MySamplerQuery.from_config(**{**period, 'pvlist': pvs}))
                if cmd == 'mydata':
                    queries.append(MyDataQuery.from_config(**{**period, 'pvlist': pvs}))
                else:
                    raise RuntimeError(f"Unrecognized subcommand.  Valid subcommands = {valid_subs}")
    return cmd, queries


def generate_mysampler_queries(begin: datetime, num_samples: int, interval: str, query_interval: int, num_queries: int,
                               pvlist: List[str], **query_kws) -> List[MySamplerQuery]:
    queries = []
    q_int = timedelta(seconds=query_interval)
    for i in range(num_queries):
        start = begin + i * q_int
        queries.append(MySamplerQuery(start=start, num_samples=num_samples, interval=interval, pvlist=pvlist,
                                      **query_kws))

    return queries


def generate_mydata_queries(begin: datetime, duration: int, query_interval: int, num_queries: int,
                            pvlist: List[str], single_pvs: bool, **query_kws) -> List[MyDataQuery]:
    queries = []
    q_int = timedelta(seconds=query_interval)
    for i in range(num_queries):
        start = begin + i * q_int
        end = start + timedelta(seconds=duration)
        if single_pvs:
            for pv in pvlist:
                queries.append(MyDataQuery(begin=start, end=end, pvlist=[pv], **query_kws))
        else:
            queries.append(MyDataQuery(begin=start, end=end, pvlist=pvlist, **query_kws))

    return queries


def main():
    parser = argparse.ArgumentParser(prog=app_name,
                                     description="""A tool for making multiple calls to mySampler in parallel.  This
    allows for the user to specify multiple queries that follow a similar pattern.  For example, query 10 minutes of
    data at one second intervals once every hour starting on a given date.""")
    subparsers = parser.add_subparsers(dest='cmd')

    cfg = subparsers.add_parser('config', help='State what to query in a config file')
    cfg.add_argument('file', help="A config file defining the command, queries, and pv list", type=str)
    cfg.add_argument('-o', '--output-file', help='File where output is saved', required=True, type=str)

    mysampler_parser = subparsers.add_parser('mysampler', help='Run mySampler on a set of queries.')
    mysampler_parser.add_argument('-b', '--begin', help="The start time from which all queries are offset",
                                  required=True,
                                  type=datetime.fromisoformat)
    mysampler_parser.add_argument('-n', '--num-samples', help='The number of samples for each query', type=int,
                                  required=True)
    mysampler_parser.add_argument('-i', '--sample-interval',
                                  help='The interval between samples in mySampler terms, e.g., "1s"',
                                  type=str, required=True)
    mysampler_parser.add_argument('-q', '--query-interval',
                                  help='The time between the start of successive queries in seconds',
                                  type=int, required=True)
    mysampler_parser.add_argument('--num-queries',
                                  help='The number of queries to make, each space --query-interval from the last.',
                                  required=True, type=int)
    mysampler_parser.add_argument('-o', '--output-file', help='File where output is saved', required=True, type=str)
    mysampler_parser.add_argument('-m', '--mya-deployment', help="MYA deployment to query (e.g. ops, history, etc.)",
                                  type=str, default=None)
    mysampler_ex = mysampler_parser.add_mutually_exclusive_group(required=True)
    mysampler_ex.add_argument('-p', '--pv-list', nargs='+', type=str, help="Space separated list of PVs to sample")
    mysampler_ex.add_argument('-f', '--pv-file', type=str,
                              help='Path to file containing PVs to sample, one PV per line.')
    mydata_parser = subparsers.add_parser('mydata', help='Run myData on a set of queries.')
    mydata_parser.add_argument('-b', '--begin', help="The start time from which all queries are offset",
                               required=True,
                               type=datetime.fromisoformat)
    mydata_parser.add_argument('-d', '--duration',
                               help='The duration in seconds of each query.  E.g., end = begin + duration',
                               type=int, required=True)
    mydata_parser.add_argument('-q', '--query-interval',
                               help='The time between the start of successive queries in seconds',
                               type=int, required=True)
    mydata_parser.add_argument('--num-queries',
                               help='The number of queries to make, each space --query-interval from the last.',
                               required=True, type=int)
    mydata_parser.add_argument('-o', '--output-file', help='File where output is saved', required=True, type=str)
    mydata_parser.add_argument('-s', '--single-pvs', help='Should myData make a single query per PV',
                               action='store_true', default=False)
    mydata_parser.add_argument('-m', '--mya-deployment', help="MYA deployment to query (e.g. ops, history, etc.)",
                               type=str, default=None)
    mydata_ex = mydata_parser.add_mutually_exclusive_group(required=True)
    mydata_ex.add_argument('-p', '--pv-list', nargs='+', type=str, help="Space separated list of PVs to sample")
    mydata_ex.add_argument('-f', '--pv-file', type=str,
                           help='Path to file containing PVs to sample, one PV per line.')

    subparsers.required = True
    args = parser.parse_args()
    if args.num_queries < 1:
        print("Error: number of queries must be at least 1")
        exit(1)

    if args.cmd == 'config':
        cmd, queries = process_input_file(args.file)
    elif args.cmd == 'mysampler':
        cmd = args.cmd
        if args.pv_file is None:
            pvlist = args.pv_list
        else:
            with open(args.pv_file, mode='r') as f:
                pvlist = [line.strip() for line in f.readlines()]
        queries = generate_mysampler_queries(begin=args.begin, num_samples=args.num_samples,
                                             interval=args.sample_interval, query_interval=args.query_interval,
                                             num_queries=args.num_queries, pvlist=pvlist,
                                             deployment=args.mya_deployment)

    elif args.cmd == 'mydata':
        cmd = args.cmd
        if args.pv_file is None:
            pvlist = args.pv_list
        else:
            with open(args.pv_file, mode='r') as f:
                pvlist = [line.strip() for line in f.readlines()]
        queries = generate_mydata_queries(begin=args.begin, duration=args.duration, query_interval=args.query_interval,
                                          num_queries=args.num_queries, pvlist=pvlist, single_pvs=args.single_pvs,
                                          deployment=args.mya_deployment)
    else:
        raise RuntimeError(f"Unsupported subcommand {args.cmd}")

    if cmd == "mysampler":
        df = do_parallel_queries(mySampler, queries)
        df.to_csv(f"{args.output_file}", index=False)
    elif cmd == "mydata":
        df = do_parallel_queries(myData, queries)
        df.to_csv(f"{args.output_file}", index=False)

    exit(0)


if __name__ == "__main__":
    main()
