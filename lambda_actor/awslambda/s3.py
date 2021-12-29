import logging
import sys
import os
import csv
import pandas as pd
from acdatalake.util.time import created_at
from datetime import datetime, timedelta, timezone
import json
from decimal import Decimal

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
       if isinstance(obj, Decimal):
           return str(obj)
       return json.JSONEncoder.default(self, obj)

def write_df(s3_client, df: pd.DataFrame, bucket: str, path: str, filename: str, is_hist:bool = False) -> str:
    """[summary]

    Args:
        s3_client ([type]): [description]
        df (pd.DataFrame): [description]
        bucket (str): [description]
        path (str): [description]
        filename (str): [description]
        is_hist (bool, optional): [description]. Defaults to False.

    Returns:
        str: [description]
    """
    if path:
        fullpath = f'{path}/{filename}'
    else:
        fullpath = f'{filename}'
    df.to_csv(f'/tmp/{filename}.csv', sep=',', encoding='utf-8', index=False, quotechar='"', quoting=csv.QUOTE_ALL)
    if is_hist:
        fullpath = f"{fullpath}_{created_at()}.csv"
        s3_client.upload_file(f'/tmp/{filename}.csv', bucket, fullpath)
    else:
        fullpath = f'{fullpath}.csv'
        s3_client.upload_file(f'/tmp/{filename}.csv', bucket, fullpath)
    return f"s3://{bucket}/{fullpath}"
    
def write_dict(s3_client, d: dict, bucket: str, path: str, filename: str, _type: str="json", is_hist:bool = False) -> str:
    if path:
        fullpath = f'{path}/{filename}'
    else:
        fullpath = f'{filename}'
    with open(f'/tmp/{filename}.{_type}', 'w') as fp:
        json.dump(d, fp, ensure_ascii=False, cls=DecimalEncoder)
    if is_hist:
        fullpath = f"{fullpath}_{created_at()}.{_type}"
        s3_client.upload_file(f'/tmp/{filename}.{_type}', bucket, fullpath)
    else:
        fullpath = f'{fullpath}.{_type}'
        s3_client.upload_file(f'/tmp/{filename}.{_type}', bucket, fullpath)
    return f"s3://{bucket}/{fullpath}"
    
def read_df(s3_client, bucket: str, path: str, filename: str, created_at:datetime = None) -> pd.DataFrame:
    """[summary]

    Args:
        s3_client ([type]): [description]
        bucket (str): [description]
        path (str): [description]
        filename (str): [description]
        created_at (datetime, optional): [description]. Defaults to None.

    Returns:
        pd.DataFrame: [description]
    """
    if path:
        fullpath = f'{path}/{filename}'
    else:
        fullpath = f'{filename}'
    if created_at:
        s3_client.download_file(bucket, f'{fullpath}_{created_at.strftime("%Y%m%d")}.csv', f'/tmp/{filename}.csv')
        df = pd.read_csv(f'/tmp/{filename}.csv')
    else:
        s3_client.download_file(bucket, f'{fullpath}.csv', f'/tmp/{filename}.csv')
        df = pd.read_csv(f'/tmp/{filename}.csv')
    return df
