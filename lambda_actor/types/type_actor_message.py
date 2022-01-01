from dataclasses import dataclass
from enum import Enum
from lambda_actor.utils.dateutil import timestamp
from typing import List
from dataclasses_json import dataclass_json
import boto3

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
    executor_id: str
    driver_trigger_timestamp: str = timestamp()

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
    driver_trigger_timestamp: str
    executor_id: int
    executor_trigger_timestamp: str = timestamp()

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
        #return f"{self.status.value},{self.message},{self.driver_trigger_timestamp},{self.executor_id},{self.executor_trigger_timestamp}"

@dataclass_json
@dataclass
class ExecutorTaskMessage:
    """[summary]
    """
    message: str
    retry_count: int = 0
    driver_start_timestamp: str = timestamp()
    
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
    def create_message(cls, executor_message_list) -> str:
            """[summary]

            Returns:
                str: [description]
            """
            return list(map(lambda m: ExecutorTaskMessage(message=m), executor_message_list))


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
    executor_id: int
    execute_time: str