from datetime import datetime, timedelta, date

def date_check() -> str:
    """
    Returns today's date as a STRING, can be used as an object
    """
    today = date.today()
    return str(today)

def dashes_to_underscores(s):
    """
    Replaces dashes to underscores IE:

    glow-in-the-dark -> glow_in_the_dark
    """
    return s.replace("-", "_")

def column_add(a,b) -> int:
     return  a.__add__(b)

def date_minus_weeks(week_num:int) -> str:
    """
    Returns a timedelta STRING

    args:

    - week_num(Integer): number of weeks to go back in time from todays current date
    """
    return str(date.today() - timedelta(weeks = week_num))