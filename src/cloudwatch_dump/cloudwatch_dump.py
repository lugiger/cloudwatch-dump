#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import pickle

import boto3
from typing import List
from datetime import timedelta
from util import RichDateTime


def get_time_range(time_str, interval):
    """
    Returns pair of RichDateTime.
    """
    d = timedelta(minutes=interval)
    if time_str:
        # parse string as localtime
        t = RichDateTime.strptime(time_str, '%Y%m%d%H%M')
    else:
        # get current time in localtime
        t = RichDateTime.from_datetime((RichDateTime.now() % d) - d)
    return t, RichDateTime.from_datetime(t + d)


def get_metrics(region: str, namespace: str, dimensions: List[dict]):
    """
    Get all metrics in specified region.
    """
    client = boto3.client('cloudwatch')
    if not client:
        raise Exception('Failed to connect to region: %s' % region)
    buf = []
    next_token = None
    while True:
        if next_token:
            result = client.list_metrics(Namespace=namespace,
                                         Dimensions=dimensions,
                                         NextToken=next_token)
        else:
            result = client.list_metrics(Dimensions=dimensions, Namespace=namespace)
        buf += list(result.get('Metrics'))
        if not result.get('next_token'):
            break
        next_token = result.next_token
    return buf


def get_data(metrics, statistics_list, start, end, period_in_sec):
    """
    Get summerized CloudWatch metric status,
    then generate tuples of metric, statistics, value, and timestamp in UTC.
    """
    client = boto3.client('cloudwatch')
    if not client:
        raise Exception('Failed to connect')

    p = timedelta(minutes=period_in_sec)

    metric_data = [{'Id': f'{stat.lower()}{index}', 'MetricStat': {'Metric': metric, 'Period': period_in_sec, 'Stat': stat}} for index, metric in enumerate(metrics) for stat in statistics_list]

    data = client.get_metric_data(
        MetricDataQueries=metric_data,
        StartTime=start,
        EndTime=end,
    )

    return [{'Data': datapoint, 'Metric': metric_data[index]['MetricStat']['Metric'] } for index, datapoint in enumerate(data['MetricDataResults'])]


def parse_args():
    """
    Parse command line arguments
    """
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument(
        '--region', dest='region', default='us-east-1', type=str,
        help='the name of the region to connect to'
    )
    parser.add_argument(
        '--time', dest='time', default=None, type=str,
        help='start time of the query in format "YYYYMMDDhhmm" localtime'
    )
    parser.add_argument(
        '--interval', dest='interval', default=60, type=int,
        help='minutes of time range in the query'
    )
    parser.add_argument(
        '--period', dest='period', default=60, type=int,
        help='seconds to aggregate in the query'
    )
    parser.add_argument(
        '--resolve', action='store_true', dest='resolve', default=False,
        help='replaces name tag value for EC2 instances instead of the instance id (default: False)'
    )
    parser.add_argument(
        '--check', action='store_true', dest='check', default=False,
        help='prints only the metrics and its statistics methods (default: False)'
    )
    parser.add_argument(
        '--namespace', dest='namespace', default='', type=str,
        help='metrics namespace to dump (default: all namespaces)'
    )
    parser.add_argument(
        '--dimensions', dest='dimensions', type=json.loads,
        help='Filter metrics, usage: {"Name":"STRING","Value":"STRING"}'
    )
    parser.add_argument(
        '--filename', dest='filename', type=str,
        help='File to save data'
    )
    return parser.parse_args()


def main():
    """
    Main function
    """
    statistics_list = ['Average', 'Sum']

    # get command line arguments
    options = parse_args()

    # calculate time range
    start, end = get_time_range(options.time, options.interval)

    # get metrics list
    metrics = get_metrics(options.region, options.namespace, options.dimensions)
    query_params = ((m, s) for m in metrics for s in statistics_list)

    if options.check:
        # print all query params when check mode
        print('start : %s' % start)
        print('end   : %s' % end)
        print('period: %s min' % options.period)
        for q in query_params:
            print('will collect metric: %s %s' % (q[0], q[1]))
    else:
        # fetch and print metrics statistics
        data = get_data(metrics, statistics_list, start, end, options.period)
        with open(options.filename, 'wb') as f:
            pickle.dump(data, f)
            print('Wrote results to file: %s' % options.filename)
            print(data)
    return 0


if __name__ == '__main__':
    main()
