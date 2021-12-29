from dataclasses import dataclass
from enum import Enum
from datetime import datetime, timedelta, timezone, date
from decimal import Decimal
import time
import json


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

class JobStatusMessageStatus(Enum):
    INIT = "init"
    FINISH = "finish"

@dataclass
class JobStatusMessage:
    """[summary]
    """
    status: JobStatusMessageStatus
    message: str
    timestamp: str = timestamp()

    @classmethod
    def decode(cls, message_str: str):
        """[summary]

        Args:
            message_str ([type]): [description]

        Returns:
            JobStatusMessage: [description]
        """
        ss = message_str.split(",")
        raw_status = ss[0]
        if raw_status == JobStatusMessageStatus.INIT.value:
            status = JobStatusMessageStatus.INIT
        elif raw_status == JobStatusMessageStatus.FINISH.value:
            status = JobStatusMessageStatus.FINISH
        else:
            raise Exception(f"JobStatusMessage perse error: {message_str}")
        message = ss[1]
        timestamp = ss[2]

        return JobStatusMessage(
            status = status,
            message = message,
            timestamp = timestamp
        )
        
    def encode(self) -> str:
        """[summary]

        Returns:
            str: [description]
        """
        return f"{self.status.value},{self.message},{self.timestamp}"


class JobTargetMessageStatus(Enum):
    INIT = "init"
    RETRY = "retry"

@dataclass
class JobTargetMessage:
    """[summary]
    """
    status: JobTargetMessageStatus
    message: str
    retry_count: int
    timestamp: str = timestamp()

    @classmethod
    def decode(cls, message_str: str):
        """[summary]

        Args:
            message_str (str): [description]

        Raises:
            Exception: [description]

        Returns:
            JobTargetMessageStatus: [description]
        """
        ss = message_str.split(",")
        raw_status = ss[0]
        if raw_status == JobTargetMessageStatus.INIT.value:
            status = JobTargetMessageStatus.INIT
        elif raw_status == JobTargetMessageStatus.RETRY.value:
            status = JobTargetMessageStatus.RETRY
        else:
            raise Exception(f"JobTargetMessage perse error: {message_str}")
        message = ss[1]
        retry_count = int(ss[2])
        timestamp = ss[3]

        return JobTargetMessage(
            status = status,
            message = message,
            retry_count = retry_count,
            timestamp = timestamp
        )
        
    def encode(self) -> str:
        """[summary]

        Returns:
            str: [description]
        """
        return f"{self.status.value},{self.message},{self.retry_count},{self.timestamp}"

def lambda_handler(event, context):
    #s3_client = boto3.client('s3')
    
    s1 = JobStatusMessage(JobStatusMessageStatus.INIT, "hello")
    s2 = JobTargetMessage(JobTargetMessageStatus.INIT, "hello", 0)
    
    print(s1.encode())
    print(s2.encode())
    
    print(JobStatusMessage.decode(s1.encode()))
    print(JobTargetMessage.decode(s2.encode()))
    
    
    #r = actor_starter("rescala-configuration", "mangawalk/mangawalk_actor", "mangawalk_actor.json")
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
