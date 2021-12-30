from dataclasses import dataclass
from enum import Enum
from lambda_actor.utils.dateutil import timestamp
from typing import List

class DriverTriggerStatusType(Enum):
    INIT = "init"
    EXECUTOR_FINISH = "executor_finish"

@dataclass
class DriverTriggerMessage:
    """[summary]
    """
    status: DriverTriggerStatusType
    message: str
    driver_trigger_timestamp: str = timestamp()

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
        if raw_status == DriverTriggerStatusType.INIT.value:
            status = DriverTriggerStatusType.INIT
        elif raw_status == DriverTriggerStatusType.EXECUTOR_FINISH.value:
            status = DriverTriggerStatusType.EXECUTOR_FINISH
        else:
            raise Exception(f"DriverTriggerStatusType parse error: {message_str}")
        message = ss[1]
        driver_trigger_timestamp = ss[2]

        return DriverTriggerMessage(
            status=status,
            message=message,
            driver_trigger_timestamp=driver_trigger_timestamp
        )
        
    def encode(self) -> str:
        """[summary]

        Returns:
            str: [description]
        """
        return f"{self.status.value},{self.message},{self.driver_trigger_timestamp}"


class ExecutorTriggerStatusType(Enum):
    START = "start"

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
        ss = message_str.split(",")
        raw_status = ss[0]
        if raw_status == ExecutorTriggerStatusType.START.value:
            status = ExecutorTriggerStatusType.START
        else:
            raise Exception(f"ExecutorTriggerStatusType parse error: {message_str}")
        message = ss[1]
        driver_trigger_timestamp = ss[2]
        executor_id = int(ss[3])
        executor_trigger_timestamp = ss[4]

        return ExecutorTriggerMessage(
            status=status,
            message=message,
            driver_trigger_timestamp=driver_trigger_timestamp,
            executor_id=executor_id,
            executor_trigger_timestamp=executor_trigger_timestamp
        )
        
    def encode(self) -> str:
        """[summary]

        Returns:
            str: [description]
        """
        return f"{self.status.value},{self.message},{self.driver_trigger_timestamp},{self.executor_id},{self.executor_trigger_timestamp}"

@dataclass
class ExecutorTaskMessage:
    """[summary]
    """
    message: str
    retry_count: int = 0
    executor_task_timestamp: str = timestamp()

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
        ss = message_str.split(",")

        message = ss[0]
        retry_count = int(ss[1])
        executor_task_timestamp = ss[2]

        return ExecutorTaskMessage(
            message=message,
            retry_count=retry_count,
            executor_task_timestamp=executor_task_timestamp
        )
        
    def encode(self) -> str:
        """[summary]

        Returns:
            str: [description]
        """
        return f"{self.message},{self.retry_count},{self.executor_task_timestamp}"

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
    message: str
    driver_trigger_timestamp: str
    executor_trigger_timestamp: str
    retry_count: int
    executor_id: int
    execute_time: str



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
        if raw_status == ExecutorResultStatusType.SUCCESS.value:
            status = ExecutorResultStatusType.SUCCESS
        elif raw_status == ExecutorResultStatusType.FAILED.value:
            status = ExecutorResultStatusType.FAILED
        else:
            raise Exception(f"ExecutorResultStatusType parse error: {message_str}")
        message = ss[1]
        driver_trigger_timestamp = ss[2]
        executor_trigger_timestamp = ss[3]
        retry_count = ss[4]
        executor_id = ss[5]
        execute_time = ss[6]

        return ExecutorResultMessage(
            status=status,
            message=message,
            driver_trigger_timestamp=driver_trigger_timestamp,
            executor_trigger_timestamp=executor_trigger_timestamp,
            retry_count=retry_count,
            executor_id=executor_id,
            execute_time=execute_time
        )
        
    def encode(self) -> str:
        """[summary]

        Returns:
            str: [description]
        """
        return f"{self.status.value},{self.message},{self.driver_trigger_timestamp},{self.executor_trigger_timestamp},{self.retry_count},{self.executor_id},{self.execute_time}"