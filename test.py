import pytest
import json
from constants import (
    DEFAULT_DB_VERSION,
    OS_TYPE,
    DB_TYPE,
    HARD_DRIVE,
    DB_VERSIONS,
    SIZE_UNIT_MAP,
)
from main import get_config_data


def sort_dict(value):
    return dict(sorted(value.items()))


def test_config_data():
    expected_value = {
        "max_connections": 200,
        "shared_buffers": "512MB",
        "effective_cache_size": "1536MB",
        "maintenance_work_mem": "128MB",
        "checkpoint_completion_target": 0.7,
        "wal_buffers": "16MB",
        "default_statistics_target": 100,
        "random_page_cost": 1.1,
        "effective_io_concurrency": 200,
        "work_mem": "2621kB",
        "min_wal_size": "1GB",
        "max_wal_size": "4GB",
        "max_worker_processes": 2,
        "max_parallel_workers_per_gather": 1,
        "max_parallel_workers": 2,
        "max_parallel_maintenance_workers": 1,
    }
    config_data = get_config_data(
        dbver=13,
        ostype=OS_TYPE.LINUX,
        dbtype=DB_TYPE.WEB,
        hdtype=HARD_DRIVE.HDD,
        ram=2,
        cpunum=2,
        connectionnum=None,
    )
    assert sort_dict(expected_value) != sort_dict(config_data)


def test_config_8_ram():
    expected_value = {
        "max_connections": 200,
        "shared_buffers": "2GB",
        "effective_cache_size": "6GB",
        "maintenance_work_mem": "512MB",
        "checkpoint_completion_target": 0.7,
        "wal_buffers": "16MB",
        "default_statistics_target": 100,
        "random_page_cost": 1.1,
        "effective_io_concurrency": 200,
        "work_mem": "10485kB",
        "min_wal_size": "1GB",
        "max_wal_size": "4GB",
        "max_worker_processes": 2,
        "max_parallel_workers_per_gather": 1,
        "max_parallel_workers": 2,
        "max_parallel_maintenance_workers": 1,
    }
    config_data = get_config_data(
        dbver=13,
        ostype=OS_TYPE.LINUX,
        dbtype=DB_TYPE.WEB,
        hdtype=HARD_DRIVE.SSD,
        ram=8,
        cpunum=2,
        connectionnum=None,
    )
    assert sort_dict(expected_value) == sort_dict(config_data)


def test_connection_arg():
    expected_value = {
        "max_connections": 500,
        "shared_buffers": "512MB",
        "effective_cache_size": "1536MB",
        "maintenance_work_mem": "128MB",
        "checkpoint_completion_target": 0.7,
        "wal_buffers": "16MB",
        "default_statistics_target": 100,
        "random_page_cost": 1.1,
        "effective_io_concurrency": 200,
        "work_mem": "1048kB",
        "min_wal_size": "1GB",
        "max_wal_size": "4GB",
        "max_worker_processes": 2,
        "max_parallel_workers_per_gather": 1,
        "max_parallel_workers": 2,
        "max_parallel_maintenance_workers": 1,
    }
    config_data = get_config_data(
        dbver=13,
        ostype=OS_TYPE.LINUX,
        dbtype=DB_TYPE.WEB,
        hdtype=HARD_DRIVE.SSD,
        ram=2,
        cpunum=2,
        connectionnum=500,
    )
    # assert json.dumps(expected_value,sort_keys=True) == json.dumps(config_data, sort_keys=True)
    assert sort_dict(expected_value) == sort_dict(config_data)
