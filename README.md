# GPUs consumption stats retriever

## Description

This tool is used to retrieve prometheus stats about GPU consumption on the IC Cluster (k8s).

## Installation

You should probably use a virtual environment. In order to do so, you can:

```shell
python -m venv venv
source venv/bin/activate
```

Then you can install the dependencies:

```shell
pip install -r requirements.txt
```

## Usage

First you should a couple of parameters into a `.env` file.

A typical `.env` file looks like this:

```text
PROMETHEUS_URL=http://prometheus.example.com/api/v1/query_range
START=2022-01-01T00:00:00.000Z
END=2022-02-28T23:59:59.999Z
STEP=15m
```

Then you can run the tool:

```shell
python main.py
```

It will generate a `stats.xls` file in the current directory with the stats.
