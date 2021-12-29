from datetime import datetime, timedelta, timezone, date
from decimal import Decimal
import time
import calendar

def daterange(_start, _end):
    for n in range((_end - _start).days):
        yield _start + timedelta(n)

def created_at(is_month: bool =False):
    JST = timezone(timedelta(hours=+9), 'JST')
    timestamp = Decimal(time.time())
    today = datetime.now(JST).date()
    if is_month:
        created_at = str(today.strftime("%Y%m"))
    else:
        created_at = str(today.strftime("%Y%m%d"))
    return created_at

def timestamp():
    now = datetime.now(timezone(timedelta(hours=9))) # 日本時刻
    s = now.strftime('%Y/%m/%d %H:%M:%S')  # yyyyMMddHHmmss形式で出力
    return s
    
def today():
    return created_at()

def tomorrow():
    JST = timezone(timedelta(hours=+9), 'JST')
    today = datetime.now(JST).date()
    return str((today + timedelta(days=1)).strftime("%Y%m%d"))
    
def yesterday():
    JST = timezone(timedelta(hours=+9), 'JST')
    today = datetime.now(JST).date()
    return str((today - timedelta(days=1)).strftime("%Y%m%d"))
    

def times():
    JST = timezone(timedelta(hours=+9), 'JST')
    timestamp = Decimal(time.time())
    today = datetime.now(JST).date()
    weekday = today.strftime('%a')
    created_at = str(today.strftime("%Y%m%d"))
    created_at_unix = Decimal(time.mktime(today.timetuple()))
    last_created_at = str((today - timedelta(days=1)).strftime("%Y%m%d"))
    return {"timestamp": timestamp, "created_at": created_at, "created_at_unix": created_at_unix,\
            "weekday": weekday, "last_created_at": last_created_at}

def get_first_date2(year, month):
    return date(int(year), int(month), 1)
    
def get_last_date2(year, month):
    return date(int(year), int(month), calendar.monthrange(int(year), int(month))[1])