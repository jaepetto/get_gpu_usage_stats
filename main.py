import json
import logging
from os import getenv
from typing import List

import arrow
import requests
import xlwt
from dotenv import load_dotenv

load_dotenv()  # Loads environment variables from the .env file

logger = logging.getLogger(__name__)


def main() -> None:
    params = get_parameters()
    allocated_gpus_stats = get_allocated_gpus_stats(params)
    used_gpus_stats = get_used_gpus_stats(params)
    gpu_count = get_gpu_count(params)
    consolidated_stats = consolidate_stats(
        [allocated_gpus_stats, used_gpus_stats, gpu_count]
    )
    dump_to_excel(consolidated_stats)


def dump_to_excel(stats: List[dict]) -> None:
    """
    Dump the stats to an Excel file
    """
    logger.info("Dumping to Excel")
    try:
        wb = xlwt.Workbook()
        ws = wb.add_sheet("Sheet 1")

        date_style = xlwt.easyxf(num_format_str='YYYY-MM-DD HH:MM:SS')

        # Write the headers
        ws.write(0, 0, "Timestamp")
        ws.write(0, 1, "Allocated GPUs")
        ws.write(0, 2, "Used GPUs")
        ws.write(0, 3, "GPU Count")

        # Write the data
        row = 1
        for stat in stats:
            ws.write(row, 0, arrow.get(stat["timestamp"]).naive, date_style)
            ws.write(row, 1, stat["allocated_gpus"])
            ws.write(row, 2, stat["used_gpus"])
            ws.write(row, 3, stat["gpu_count"])
            row += 1

        wb.save("stats.xls")
    except Exception as e:
        logger.error(e)
        raise e


def consolidate_stats(stats: List[List[dict]]) -> List[dict]:
    """
    Consolidate the stats from the Prometheus API into a single list
    """
    logger.info("Consolidating stats")
    try:
        return_value = []

        # get the list of timestamps
        timestamps = [stat["timestamp"] for stat in stats[0]]

        # iterate through the timestamps to collect the corresponding stats
        for timestamp in timestamps:
            return_value.append(
                {
                    "timestamp": timestamp,
                    "allocated_gpus": [
                        item["value"]
                        for item in stats[0]
                        if item["timestamp"] == timestamp
                    ][0],
                    "used_gpus": [
                        item["value"]
                        for item in stats[1]
                        if item["timestamp"] == timestamp
                    ][0],
                    "gpu_count": [
                        item["value"]
                        for item in stats[2]
                        if item["timestamp"] == timestamp
                    ][0],
                }
            )

        return return_value
    except Exception as e:
        logger.error(e)
        raise e


def get_parameters() -> dict:
    """
    Get parameters from the command line
    """
    logger.info("Getting parameters")
    try:
        return_value = {}
        return_value["url"] = getenv("PROMETHEUS_URL")
        return_value["start"] = getenv(
            "START", arrow.utcnow().shift(days=-1).format("YYYY-MM-DDTHH:mm:ss.SSSZZ")
        )
        return_value["end"] = getenv(
            "END", arrow.utcnow().format("YYYY-MM-DDTHH:mm:ss.SSSZZ")
        )
        return_value["step"] = getenv("STEP", "15m")

        return return_value
    except Exception as e:
        logger.error(e)
        raise e


def get_allocated_gpus_stats(params: dict) -> List[dict]:
    logger.info("Getting allocated GPU stats")
    try:
        url = params["url"]

        querystring = {
            "query": 'sum(max(runai_allocated_gpus) by (pod_group_uuid) * on (pod_group_uuid)(runai_pod_group_phase_with_info{phase=~"Running"}==1)) or vector (0)',
            "start": params["start"],
            "end": params["end"],
            "step": params["step"],
        }
        payload = ""
        response = requests.request("GET", url, data=payload, params=querystring)

        stats = parse_stats(response.text)
        return stats
    except Exception as e:
        logger.error(e)
        raise e


def get_used_gpus_stats(params: dict) -> List[dict]:
    logger.info("Getting used GPU stats")
    try:
        url = params["url"]

        querystring = {
            "query": "avg(runai_node_gpu_utilization) or vector(0)",
            "start": params["start"],
            "end": params["end"],
            "step": params["step"],
        }
        payload = ""
        response = requests.request("GET", url, data=payload, params=querystring)

        stats = parse_stats(response.text)
        return stats
    except Exception as e:
        logger.error(e)
        raise e


def get_gpu_count(params: dict) -> List[dict]:
    logger.info("Getting used GPU stats")
    try:
        url = params["url"]

        querystring = {
            "query": "count(runai_node_gpu_last_not_idle_time) or vector(0)",
            "start": params["start"],
            "end": params["end"],
            "step": params["step"],
        }
        payload = ""
        response = requests.request("GET", url, data=payload, params=querystring)

        stats = parse_stats(response.text)
        return stats
    except Exception as e:
        logger.error(e)
        raise e


def get_timestamp(timestamp: str) -> str:
    """
    return timestamp in the format of YYYY-MM-DDTHH:MM:SS.000Z
    """
    logger.info("Getting timestamp")
    try:
        return arrow.get(timestamp).format()
    except Exception as e:
        logger.error(e)
        raise e


def parse_stats(raw_stats: str) -> dict:
    """
    Parse the raw stats from the Prometheus API and return a dictionary
    """
    logger.info("Parsing stats")
    try:
        stats = json.loads(raw_stats)
        if stats["status"] == "success":
            result = []
            raw_data = stats["data"]["result"][0]["values"]
            [
                result.append(
                    {"timestamp": get_timestamp(timestamp=timestamp), "value": float(value)}
                )
                for timestamp, value in raw_data
            ]
            return result

        else:
            logger.error(stats["error"])
            raise Exception(stats["error"])
    except Exception as e:
        logger.error(e)
        raise e


if __name__ == "__main__":
    main()
