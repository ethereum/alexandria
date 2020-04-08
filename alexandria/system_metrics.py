from typing import (
    NamedTuple,
)

import psutil


class CpuStats(NamedTuple):

    # Time spent on all processes
    global_time: int
    # Time spent waiting on IO
    global_wait_io: int


class DiskStats(NamedTuple):

    # Number of read operations executed
    read_count: int
    # Number of bytes read
    read_bytes: int
    # Number of write operations executed
    write_count: int
    # Number of bytes written
    write_bytes: int


class NetworkStats(NamedTuple):

    # Number of network packets sent
    out_packets: int
    # Number of network packets received
    in_packets: int


class SystemStats(NamedTuple):
    cpu_stats: CpuStats
    disk_stats: DiskStats
    network_stats: NetworkStats


def read_cpu_stats() -> CpuStats:
    stats = psutil.cpu_times()
    return CpuStats(
        global_time=int(stats.user + stats.nice + stats.system),
        global_wait_io=int(stats.iowait),
    )


def read_disk_stats() -> DiskStats:
    stats = psutil.disk_io_counters()
    return DiskStats(
        read_count=stats.read_count,
        read_bytes=stats.read_bytes,
        write_count=stats.write_count,
        write_bytes=stats.write_bytes,
    )


def read_network_stats() -> NetworkStats:
    stats = psutil.net_io_counters()
    return NetworkStats(
        in_packets=stats.packets_recv,
        out_packets=stats.packets_sent
    )
