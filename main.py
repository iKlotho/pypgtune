import argparse
from math import floor, ceil
from constants import (
    DEFAULT_DB_VERSION,
    OS_TYPE,
    DB_TYPE,
    HARD_DRIVE,
    DB_VERSIONS,
    SIZE_UNIT_MAP,
    KB_UNIT_MAP,
)


def format_value(value):
    def get_result():
        if value % KB_UNIT_MAP["KB_PER_GB"] == 0:
            return {"value": floor(value / KB_UNIT_MAP["KB_PER_GB"]), "unit": "GB"}
        if value % KB_UNIT_MAP["KB_PER_MB"] == 0:
            return {"value": floor(value / KB_UNIT_MAP["KB_PER_MB"]), "unit": "MB"}

        return {"value": value, "unit": "kB"}

    result = get_result()

    return f"{result['value']}{result['unit']}"


def total_memory_in_bytes(total_memory: int, total_memory_unit: str) -> int:
    return total_memory * SIZE_UNIT_MAP[total_memory_unit]


def total_memory_in_kbytes(total_memory_bytes: int) -> int:
    return total_memory_bytes / SIZE_UNIT_MAP["KB"]


def get_db_default_values(db_version: DB_VERSIONS) -> dict:
    values = {
        9.2: {},
        9.3: {},
        9.4: {},
        9.5: {"max_worker_processes": 8},
        9.6: {"max_worker_processes": 8, "max_parallel_workers_per_gather": 0},
        10: {
            "max_worker_processes": 8,
            "max_parallel_workers_per_gather": 2,
            "max_parallel_workers": 8,
        },
        11: {
            "max_worker_processes": 8,
            "max_parallel_workers_per_gather": 2,
            "max_parallel_workers": 8,
        },
        12: {
            "max_worker_processes": 8,
            "max_parallel_workers_per_gather": 2,
            "max_parallel_workers": 8,
        },
        13: {
            "max_worker_processes": 8,
            "max_parallel_workers_per_gather": 2,
            "max_parallel_workers": 8,
        },
    }
    return values[db_version]


def total_memory_in_kb(total_memory_bytes):
    return total_memory_bytes / SIZE_UNIT_MAP["KB"]


def shared_buffers(total_memory_kb, db_type: DB_TYPE, os_type: OS_TYPE):
    shared_buffers_value = {
        DB_TYPE.WEB: floor(total_memory_kb / 4),
        DB_TYPE.OLTP: floor(total_memory_kb / 4),
        DB_TYPE.DW: floor(total_memory_kb / 4),
        DB_TYPE.DESKTOP: floor(total_memory_kb / 16),
        DB_TYPE.MIXED: floor(total_memory_kb / 4),
    }[db_type]
    win_memory_limit = 512 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"]
    if OS_TYPE.WINDOWS == os_type and shared_buffers_value > win_memory_limit:
        shared_buffers_value = win_memory_limit
    return shared_buffers_value


def maintenance_work_mem(total_memory_in_kb, db_type: DB_TYPE, os_type: OS_TYPE):
    maintenance_work_mem_value = {
        DB_TYPE.WEB: floor(total_memory_in_kb / 16),
        DB_TYPE.OLTP: floor(total_memory_in_kb / 16),
        DB_TYPE.DW: floor(total_memory_in_kb / 8),
        DB_TYPE.DESKTOP: floor(total_memory_in_kb / 16),
        DB_TYPE.MIXED: floor(total_memory_in_kb / 16),
    }[db_type]
    # Cap maintenance RAM at 2GB on servers with lots of memory
    memory_limit = 2 * SIZE_UNIT_MAP["GB"] / SIZE_UNIT_MAP["KB"]

    if maintenance_work_mem_value > memory_limit:
        if os_type == OS_TYPE.WINDOWS:
            maintenance_work_mem_value = memory_limit - (
                1 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"]
            )
        else:
            maintenance_work_mem_value = memory_limit

    return maintenance_work_mem_value


def wal_buffers(shared_buffers_value):
    # Follow auto-tuning guideline for wal_buffers added in 9.1, where it's
    # set to 3% of shared_buffers up to a maximum of 16MB.
    wal_buffers_value = floor(3 * shared_buffers_value / 100)
    max_wal_buffer = 16 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"]

    if wal_buffers_value > max_wal_buffer:
        wal_buffers_value = max_wal_buffer

    # It's nice of wal_buffers is an even 16MB if it's near that number. Since
    # that is a common case on Windows, where shared_buffers is clipped to 512MB,
    # round upwards in that situation
    wal_buffer_near_value = 14 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"]

    if wal_buffers_value > wal_buffer_near_value and wal_buffers_value < max_wal_buffer:
        wal_buffers_value = max_wal_buffer

    if wal_buffers_value < 32:
        wal_buffers_value = 32

    return wal_buffers_value


def effective_io_concurrency(os_type: OS_TYPE, hd_type: HARD_DRIVE):
    if os_type in [OS_TYPE.WINDOWS, OS_TYPE.MAC]:
        return None

    return {HARD_DRIVE.HDD: 2, HARD_DRIVE.SSD: 200, HARD_DRIVE.SAN: 300}[hd_type]


def parallel_settings(db_version: DB_VERSIONS, db_type: DB_TYPE, cpu_num: int) -> list:
    if db_version < 9.5 or cpu_num < 2:
        return []

    config = [{"key": "max_worker_processes", "value": cpu_num}]

    if db_version >= 9.6:
        workers_per_gather = ceil(cpu_num / 2)

        if db_type is not DB_TYPE.DW and workers_per_gather > 4:
            workers_per_gather = 4

        config.append(
            {"key": "max_parallel_workers_per_gather", "value": workers_per_gather}
        )

    if db_version >= 10:
        config.append({"key": "max_parallel_workers", "value": cpu_num})

    if db_version >= 11:
        parallel_maintenance_workers = ceil(cpu_num / 2)

        if parallel_maintenance_workers > 4:
            parallel_maintenance_workers = 4

        config.append(
            {
                "key": "max_parallel_maintenance_workers",
                "value": parallel_maintenance_workers,
            }
        )

    return config


def work_mem(
    total_memory_in_kb,
    shared_buffers,
    max_connections,
    parallel_settings_value,
    db_default_values,
    db_type: DB_TYPE,
):
    def get_parallel_work_mem():
        if parallel_settings_value:
            max_parallel_workers_per_gather = [
                value
                for value in parallel_settings_value
                if value["key"] == "max_parallel_workers_per_gather"
            ][0]

            if (
                max_parallel_workers_per_gather
                and max_parallel_workers_per_gather["value"]
                and max_parallel_workers_per_gather["value"] > 0
            ):
                return max_parallel_workers_per_gather["value"]

        if (
            db_default_values["max_parallel_workers_per_gather"]
            and db_default_values["max_parallel_workers_per_gather"] > 0
        ):
            return db_default_values["max_parallel_workers_per_gather"]

        return 1

    parallel_for_work_mem = get_parallel_work_mem()

    work_mem_value = (
        (total_memory_in_kb - shared_buffers)
        / (max_connections * 3)
        / parallel_for_work_mem
    )

    work_mem_result = {
        DB_TYPE.WEB: floor(work_mem_value),
        DB_TYPE.OLTP: floor(work_mem_value),
        DB_TYPE.DW: floor(work_mem_value / 2.0),
        DB_TYPE.DESKTOP: floor(work_mem_value / 6.0),
        DB_TYPE.MIXED: floor(work_mem_value / 2.0),
    }[db_type]

    # if less, than 64 kb, than set it to minimum
    if work_mem_result < 64:
        work_mem_result = 64

    return work_mem_result


def kernel_shmall(total_memory_bytes):
    return floor(total_memory_bytes / 8192)


def kernel_shmmax(kernel_shmall_value):
    return kernel_shmall_value * 4096


def get_config_data(
    dbver: int,
    ostype: OS_TYPE,
    dbtype: DB_TYPE,
    hdtype: HARD_DRIVE,
    ram: int,
    cpunum: int = None,
    connectionnum: int = None,
):
    """
    generate a config file for psql given paramaters
    """
    total_memory_unit = "GB"
    mem_in_bytes = total_memory_in_bytes(ram, total_memory_unit)
    mem_in_kbytes = total_memory_in_kbytes(mem_in_bytes)
    shared_buffers_value = shared_buffers(mem_in_kbytes, dbtype, ostype)
    max_connections = dbtype.max_connections()
    if connectionnum and connectionnum > max_connections:
        max_connections = connectionnum
    parallel_settings_value = parallel_settings(dbver, dbtype, cpunum)
    db_default_values = get_db_default_values(dbver)
    config_data = {
        "max_connections": max_connections,
        "shared_buffers": format_value(shared_buffers_value),
        "effective_cache_size": format_value(
            dbtype.effective_cache_size(mem_in_kbytes)
        ),
        "maintenance_work_mem": format_value(
            maintenance_work_mem(mem_in_kbytes, dbtype, ostype)
        ),
        "checkpoint_completion_target": dbtype.checkpoint_completion_target(),
        "wal_buffers": format_value(wal_buffers(shared_buffers_value)),
        "default_statistics_target": dbtype.default_statistics_target(),
        "random_page_cost": hdtype.random_page_cost(),
        "effective_io_concurrency": effective_io_concurrency(ostype, hdtype),
        "work_mem": format_value(
            work_mem(
                mem_in_kbytes,
                shared_buffers_value,
                max_connections,
                parallel_settings_value,
                db_default_values,
                dbtype,
            )
        ),
    }

    segments = dbtype.checkpoint_segments(dbver)

    def get_segments():
        segments_dict = {}
        for segment in segments:
            if segment["key"] == "checkpoint_segment":
                segments_dict[segment["key"]] = segment["value"]
            else:
                segments_dict[segment["key"]] = format_value(segment["value"])

        return segments_dict

    config_data.update(
        dict(map(lambda x: (x["key"], x["value"]), parallel_settings_value))
    )
    config_data.update(get_segments())
    return config_data


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enter your server details.")
    parser.add_argument(
        "--dbver",
        action="store",
        type=int,
        choices=DB_VERSIONS,
        required=True,
        default=DEFAULT_DB_VERSION,
    )
    parser.add_argument(
        "--ostype",
        action="store",
        type=OS_TYPE,
        choices=list(OS_TYPE),
        required=True,
        default=OS_TYPE.LINUX,
    )
    parser.add_argument(
        "--dbtype",
        action="store",
        type=DB_TYPE,
        choices=list(DB_TYPE),
        required=True,
        default=DB_TYPE.WEB,
    )
    parser.add_argument("--ram", action="store", type=int, required=True)
    parser.add_argument(
        "--cpunum",
        action="store",
        type=int,
        required=True,
        help="Number of CPUs, which PostgreSQL can use CPUs = threads per core * cores per socket * sockets",
    )
    parser.add_argument(
        "--connectionnum",
        action="store",
        type=int,
        required=False,
        help="Maximum number of PostgreSQL client connection",
    )
    parser.add_argument(
        "--hdtype",
        action="store",
        type=HARD_DRIVE,
        choices=list(HARD_DRIVE),
        required=True,
        default=HARD_DRIVE.SSD,
    )
    args = parser.parse_args()

    config_data = get_config_data(
        args.dbver,
        args.ostype,
        args.dbtype,
        args.hdtype,
        args.ram,
        args.cpunum,
        args.connectionnum,
    )

    print("c", config_data)
