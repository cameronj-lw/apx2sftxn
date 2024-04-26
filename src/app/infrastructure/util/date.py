"""
Date related utils
"""

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
    
    