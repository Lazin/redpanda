#!/bin/python

import argparse
import os
import re
from tabulate import tabulate
import pygal
from itertools import chain
from pygal.style import Style
from datetime import datetime
from datetime import time
import pytz

chartStyle = Style(
    background='transparent',
    plot_background='transparent',
    font_family='googlefont:Montserrat',
    colors=('#800000', '#4363d8', '#f58231'),
    label_font_size=16,
    legend_font_size=16,
    major_label_font_size=16,
)

theme = chartStyle

data_pattern = re.compile(
    ".*(?P<time>\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d).*?\[shard (?P<shard>\d)\] h(?P<type>\w)-(?P<name>.*)-\d.*hist: \{(?P<data>.*)\}"
)
clear_pattern = re.compile(
    ".*(?P<time>\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d).*?\[shard (?P<shard>\d)\] h(?P<type>\w)-(?P<name>.*)-\d.*hist: clear"
)

headers = [
    'node', 'shard', 'time', 'p10', 'p50', 'p90', 'p99', 'p999', 'max', 'cnt'
]


class DataPoint:
    def __init__(self, node, shard, time, data, t):
        # print(f">> {node} {shard} {time} : {data}")
        data_dict = dict()
        split = data.split(",")
        for kv in split:
            key, val = kv.split('=')
            data_dict[key] = val
        self.t = t
        self.node = node
        self.shard = shard
        self.time = time
        self.p10 = data_dict['p10']
        self.p50 = data_dict['p50']
        self.p90 = data_dict['p90']
        self.p99 = data_dict['p99']
        self.p999 = data_dict['p999']
        self.max = data_dict['max']
        self.cnt = data_dict['cnt']

    def row(self):
        return [
            self.node, self.shard, self.time, self.p10, self.p50, self.p90,
            self.p99, self.p999, self.max, self.cnt
        ]


class Histogram:
    def __init__(self, name, t):
        self.name = name
        self.data = []
        self.t = t

    def add(self, dp):
        self.data.append(dp)

    def tabulate(self):
        rows = []
        for dp in self.data:
            rows.append(dp.row())
        return tabulate(rows, headers=headers, tablefmt='github')

    def __str__(self) -> str:
        return f'\n\n{self.name}\n{self.tabulate()}'
    
def type_to_name(t):
    if t == 'Q':
        return 'items'
    elif t == 'L':
        return 'microseconds'
    elif t == 'M':
        return 'bytes'
    raise ValueError("Invalid type")


def create_chart(hist_name, time_series, output_dir, axis):
    print(f"creating {hist_name} in {output_dir}")
    chart = pygal.TimeLine(style=theme,
                           dots_size=2,
                           show_dots=True,
                           stroke_style={'width': 1},
                           stroke=False,
                           fill=False,
                           legend_at_bottom=True,
                           show_x_guides=False,
                           show_y_guides=True,
                           x_label_rotation=45)
    chart.title = hist_name

    chart.human_readable = True
    if 'cnt' in hist_name:
        chart.y_title = "count"
    else:
        chart.y_title = type_to_name(axis)
    chart.x_title = 'Time'

    ys = []

    for label, values, labels in time_series:
        ys.append(values)
        chart.add(label, [(datetime.utcfromtimestamp(x), y)
                          for x, y in zip(labels, values)])
    ys = chain.from_iterable(ys)
    max_y = float('-inf')  # Hack for starting w/ INT64_MIN
    for y in ys:
        if max_y < y:
            max_y = y
    chart.range = (max_y * 0.0, max_y * 1.20)
    chart.value_formatter = lambda y: "{:,.0f}".format(y)

    chart.render_to_file(f'{output_dir}/{hist_name}.svg')


def parse_time(str):
    if "secs" in str:
        return float(str.replace("secs", "")) * 1000000
    elif "ms" in str:
        return float(str.replace("ms", "")) * 1000
    elif "μs" in str:
        return float(str.replace("μs", ""))
    else:
        return float(str) * 1000000

def parse_items(str):
    return float(str)

def parse_memory(str):
    if "TiB" in str:
        return float(str.replace("TiB", "")) * 1024*1024*1024*1024
    elif "GiB" in str:
        return float(str.replace("GiB", "")) * 1024*1024*1024
    elif "MiB" in str:
        return float(str.replace("MiB", "")) * 1024*1024
    elif "KiB" in str:
        return float(str.replace("KiB", "")) * 1024
    elif "bytes" in str:
        return float(str.replace("bytes", ""))
    else:
        return float(str)


raw_data_points = {}


def main():
    data_points = dict()
    histograms = dict()
    parser = argparse.ArgumentParser(description='Print histograms summary')
    parser.add_argument('--input',
                        type=str,
                        default=".",
                        help="directory containing redpanda logs")
    parser.add_argument('--output',
                        type=str,
                        default="./hist",
                        help="output directory")
    parser.add_argument('--filename',
                        type=str,
                        default="redpanda.log",
                        help="redpanda log file name")
    args = parser.parse_args()

    # walks over directory to find all log files with given name
    # expected directories hierarchy:
    #
    #  <input_dir>/n1/redpanda.log
    #  <input_dir>/n2/redpanda.log
    #  <input_dir>/n3/redpanda.log
    #
    #  node ids are established using the directory in which the log file is found
    toycluster = ['n0', 'n1', 'n2']
    for root, dirs, files in os.walk(args.input):
        for d in dirs:
            print(f"loocking at dir: {d}")
            if d not in toycluster:
                continue
            log_file = os.path.join(root, d, args.filename)
            print(f"loocking at file: {log_file}")
            node = d
            with open(log_file, "r") as f:
                for l in f.readlines():
                    clear_match = clear_pattern.match(l)
                    if clear_match:
                        name = clear_match['name']
                        shard = clear_match['shard']
                        if (node, name, shard) in data_points and len(
                                data_points[(node, name, shard)]) > 0:
                            histograms[name].add(data_points[(node, name,
                                                              shard)].pop())
                            data_points[(node, name, shard)] = []
                        continue

                    data_match = data_pattern.match(l)
                    if data_match:
                        name = data_match['name']
                        shard = data_match['shard']
                        htype = data_match['type']
                        if name not in histograms:
                            histograms[name] = Histogram(name, htype)
                        if (node, name, shard) not in data_points:
                            data_points[(node, name, shard)] = []
                        if (node, name, shard) not in raw_data_points:
                            raw_data_points[(node, name, shard)] = []
                        raw_data_points[(node, name, shard)].append(
                            DataPoint(node, data_match['shard'],
                                      data_match['time'], data_match['data'], htype))
                        data_points[(node, name, shard)].append(
                            DataPoint(node, data_match['shard'],
                                      data_match['time'], data_match['data'], htype))
    graphs = {}
    for (node, name, shard), data in raw_data_points.items():
        for d in data:
            histograms[name].add(d)
        if name not in graphs.keys():
            graphs[name] = {}

        if (node, shard) not in graphs[name].keys():
            graphs[name][(node, shard)] = {}
            graphs[name][(node, shard)]['x'] = []
            graphs[name][(node, shard)]['y'] = []
            graphs[name][(node, shard)]['t'] = None

            for d in data:
                date = datetime.strptime(d.time, '%Y-%m-%d %H:%M:%S')
                date.replace(tzinfo=pytz.timezone("Europe/Moscow"))
                graphs[name][(node,
                              shard)]['x'].append(date.timestamp() + 7200)
                if d.t == 'L':
                    value = parse_time(d.max)
                elif d.t == 'Q':
                    value = parse_items(d.max)
                else:
                    value = parse_memory(d.max)
                if ("cnt" in name):
                    value = value
                graphs[name][(node, shard)]['y'].append(value)
                if graphs[name][(node, shard)]['t'] is None:
                    graphs[name][(node, shard)]['t'] = d.t
                elif graphs[name][(node, shard)]['t'] != d.t:
                    raise ValueError("Mixed values in histogram")

    for name, graph_data in graphs.items():
        ts = []
        xs = []
        series = []
        for (node, shard), data in graph_data.items():
            series.append(f'{node},{shard}')
            ts.append(data['y'])
            xs.append(data['x'])

        create_chart(name, zip(series, ts, xs), args.output, data['t'])

    with open(f'{args.output}/histograms.log', 'w') as f:
        for h in histograms.values():
            f.write(f"{h}")


if __name__ == "__main__":
    main()
