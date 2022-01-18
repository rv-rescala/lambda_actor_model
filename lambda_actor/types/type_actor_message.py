from dataclasses import dataclass
from enum import Enum
from lambda_actor.utils.dateutil import timestamp
from typing import List
from dataclasses_json import dataclass_json
from datetime import datetime

class DriverTriggerStatusType(Enum):
    CONTINUE = "continue"
    FINISH = "finish"

@dataclass_json
@dataclass
class DriverTriggerMessage:
    """[summary]
    """
    status: DriverTriggerStatusType
    message: str
    trigger_groupid: str
    task_groupid: str
    driver_start_timestamp: str

    @classmethod
    def decode(cls, message_str: str):
        """[summary]

        Args:
            message_str ([type]): [description]

        Returns:
            JobStatusMessage: [description]
        """
        return cls.from_json(message_str)
        
    def encode(self) -> str:
        """[summary]

        Returns:
            str: [description]
        """
        return self.to_json(indent=4, ensure_ascii=False)
       
class ExecutorTriggerStatusType(Enum):
    START = "start"
    INIT_START = "init_start"

@dataclass_json
@dataclass
class ExecutorTriggerMessage:
    """[summary]
    """
    status: ExecutorTriggerStatusType
    message: str
    trigger_groupid: str
    task_groupid: str
    executor_start_timestamp: str

    @classmethod
    def decode(cls, message_str: str):
        """[summary]

        Args:
            message_str ([type]): [description]

        Returns:
            JobStatusMessage: [description]
        """
        return cls.from_json(message_str)
        
    def encode(self) -> str:
        """[summary]

        Returns:
            str: [description]
        """
        return self.to_json(indent=4, ensure_ascii=False)
        
@dataclass_json
@dataclass
class ExecutorTaskMessage:
    """[summary]
    """
    message: str
    retry_count: int
    task_groupid: str
    driver_start_timestamp: str
    
    @classmethod
    def decode_list(cls, message_list: List[str]):
        """
        """
        return list(map(lambda m: ExecutorTaskMessage.decode(m), message_list))
        
    @classmethod
    def decode(cls, message_str: str):
        """[summary]

        Args:
            message_str (str): [description]

        Raises:
            Exception: [description]

        Returns:
            DriverType: [description]
        """
        return cls.from_json(message_str)
        
    def encode(self) -> str:
        """[summary]

        Returns:
            str: [description]
        """
        return self.to_json(indent=4, ensure_ascii=False)

    @classmethod
    def encode_list(cls, executor_task_message_list) -> str:
            """[summary]

            Returns:
                str: [description]
            """
            return list(map(lambda m: m.encode(), executor_task_message_list))

    @classmethod
    def create_message(cls, executor_message_list, task_groupid) -> str:
            """[summary]

            Returns:
                str: [description]
            """
            return list(map(lambda m: ExecutorTaskMessage(message=m, retry_count=0, task_groupid=task_groupid, driver_start_timestamp=timestamp()), executor_message_list))


class ExecutorResultStatusType(Enum):
    SUCCESS = "success"
    FAILED = "failed"

@dataclass
class ExecutorResultMessage:
    """[summary]
    """
    status: ExecutorResultStatusType
    result: str
    driver_start_timestamp: str
    executor_start_timestamp: str
    retry_count: int
    trigger_groupid: str
    task_groupid: str
    execute_time: str
    timestamp: str