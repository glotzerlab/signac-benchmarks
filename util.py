import os
import psutil


def fmt_size(size, units=None):
    "Returns a human readable string reprentation of bytes."
    if units is None:
        units = [' bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']
    return str(size) + units.pop(0) if size < 1024 else fmt_size(size >> 10, units[1:])


def get_partition(path):
    path = os.path.realpath(path)
    candidates = dict()
    for partition in psutil.disk_partitions(all=True):
        mp = os.path.realpath(partition.mountpoint)
        if path.startswith(mp):
            candidates[mp] = partition
    if candidates:
        return candidates[list(sorted(candidates, key=len))[-1]]
    else:
        raise LookupError(path)
