def fmt_size(size, units=None):
    "Returns a human readable string reprentation of bytes."
    if units is None:
        units = [' bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']
    return str(size) + units.pop(0) if size < 1024 else fmt_size(size >> 10, units[1:])


