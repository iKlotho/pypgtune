from enum import Enum
from math import floor

SIZE_UNIT_MAP = {"KB": 1024, "MB": 1048576, "GB": 1073741824, "TB": 1099511627776}
KB_UNIT_MAP = {"KB_PER_MB": 1024, "KB_PER_GB": 1048576}

SIZE_UNIT_MB = "MB"
SIZE_UNIT_GB = "GB"

DEFAULT_DB_VERSION = 13
DB_VERSIONS = [DEFAULT_DB_VERSION, 12, 11, 10, 9.6, 9.5, 9.4, 9.3, 9.2]


class OS_TYPE(Enum):
    LINUX = "linux"
    WINDOWS = "windows"
    MAC = "mac"

    def __str__(self) -> str:
        return self.value


class HARD_DRIVE(Enum):
    SSD = "ssd"
    SAN = "san"
    HDD = "hdd"

    def __str__(self):
        return self.value

    def random_page_cost(self):
        return {HARD_DRIVE.HDD: 4, HARD_DRIVE.SSD: 1.1, HARD_DRIVE.SAN: 1.1}[self]


class DB_TYPE(Enum):
    WEB = "web"
    # online transaction processing system
    OLTP = "oltp"
    # data warehouse
    DW = "dw"
    DESKTOP = "desktop"
    MIXED = "mixed"

    def __str__(self) -> str:
        return self.value

    def max_connections(self, connection_num: int = None) -> dict:
        if not connection_num:
            connection_num = {
                DB_TYPE.WEB: 200,
                DB_TYPE.OLTP: 300,
                DB_TYPE.DW: 40,
                DB_TYPE.DESKTOP: 20,
                DB_TYPE.MIXED: 100,
            }[self]
        return connection_num

    def effective_cache_size(self, total_memory_in_kb):
        return {
            DB_TYPE.WEB: floor(total_memory_in_kb * 3 / 4),
            DB_TYPE.OLTP: floor(total_memory_in_kb * 3 / 4),
            DB_TYPE.DW: floor(total_memory_in_kb * 3 / 4),
            DB_TYPE.DESKTOP: floor(total_memory_in_kb / 4),
            DB_TYPE.MIXED: floor(total_memory_in_kb * 3 / 4),
        }[self]

    def checkpoint_segments(self, db_version: DB_VERSIONS):
        if db_version < 9.5:
            return [
                {
                    key: "checkpoint_segments",
                    value: {
                        DB_TYPE.WEB: 32,
                        DB_TYPE.OLTP: 64,
                        DB_TYPE.DW: 128,
                        DB_TYPE.DESKTOP: 3,
                        DB_TYPE.MIXED: 32,
                    }[self],
                }
            ]
        else:
            return [
                {
                    "key": "min_wal_size",
                    "value": {
                        DB_TYPE.WEB: (1024 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"]),
                        DB_TYPE.OLTP: (
                            2048 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"]
                        ),
                        DB_TYPE.DW: (4096 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"]),
                        DB_TYPE.DESKTOP: (
                            100 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"]
                        ),
                        DB_TYPE.MIXED: (
                            1024 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"]
                        ),
                    }[self],
                },
                {
                    "key": "max_wal_size",
                    "value": {
                        DB_TYPE.WEB: (4096 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"]),
                        DB_TYPE.OLTP: (
                            8192 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"]
                        ),
                        DB_TYPE.DW: (16384 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"]),
                        DB_TYPE.DESKTOP: (
                            2048 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"]
                        ),
                        DB_TYPE.MIXED: (
                            4096 * SIZE_UNIT_MAP["MB"] / SIZE_UNIT_MAP["KB"]
                        ),
                    }[self],
                },
            ]

    def checkpoint_completion_target(self):
        return {
            DB_TYPE.WEB: 0.7,
            DB_TYPE.OLTP: 0.9,
            DB_TYPE.DW: 0.9,
            DB_TYPE.DESKTOP: 0.5,
            DB_TYPE.MIXED: 0.9,
        }[self]

    def default_statistics_target(self):
        return {
            DB_TYPE.WEB: 100,
            DB_TYPE.OLTP: 100,
            DB_TYPE.DW: 500,
            DB_TYPE.DESKTOP: 100,
            DB_TYPE.MIXED: 100,
        }[self]
