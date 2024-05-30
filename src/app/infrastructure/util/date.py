"""
Date related utils
"""

import datetime
from typing import Union

from infrastructure.sql_tables import LWDBCalendarTable


def format_time(t):
    """
    Get time string with only 3 decimal places for seconds

    Args:
    - t (datetime.time): Time to format
    
    Returns:
    - String representing time with seconds chopped off after 3 decimal places
    """
    s = t.strftime('%Y-%m-%d %H:%M:%S.%f')
    return s[:-3]


def get_previous_bday(ref_date):
    """
    Get the previous business date to reference date

    Args:
    - ref_date (datetime.date): Reference date
    
    Returns:
    - datetime.date: Previous biz date
    """
    calendar = LWDBCalendarTable().read_for_date(ref_date)
    prev_bday = calendar['prev_bday']
    return prev_bday[0].date()


def since_epoch_to_datetime(v) -> Union[datetime.date, datetime.datetime]:
    """
    Get datetime (or date) from a number of ms or days since 1/1/1970

    Args:
    - v (int): ms or days since 1/1/1970
    
    Returns:
    - datetime or date
    """

    if abs(v) < 220898880000:  # TODO: is this a desirable threshold to "guess" if ms or days?
        return datetime.date(year=1970, month=1, day=1) + datetime.timedelta(days=v)
    else:
        return datetime.datetime(year=1970, month=1, day=1) + datetime.timedelta(milliseconds=v/1000/60/60/24)
    
    