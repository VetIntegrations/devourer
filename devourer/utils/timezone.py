def is_aware(value):
    """
    Determine if a given datetime.datetime is aware.
    The concept is defined in Python's docs:
    https://docs.python.org/library/datetime.html#datetime.tzinfo
    Assuming value.tzinfo is either None or a proper datetime.tzinfo,
    value.utcoffset() implements the appropriate logic.
    """
    return value.utcoffset() is not None
