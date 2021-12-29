from dataclasses import dataclass
from enum import Enum
from mangawalk_actor.utils.dateutil import timestamp
from typing import List

class DriverStatusType(Enum):
    INIT = "init"
    FINISH = "finish"

@dataclass
class DriverStatusMessage:
    """[summary]
    """
    status: DriverStatusType
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
        if raw_status == DriverStatusType.INIT.value:
            status = DriverStatusType.INIT
        elif raw_status == DriverStatusType.FINISH.value:
            status = DriverStatusType.FINISH
        else:
            raise Exception(f"DriverStatusMessage parse error: {message_str}")
        message = ss[1]
        timestamp = ss[2]

        return DriverStatusMessage(
            status=status,
            message=message,
            timestamp=timestamp
        )
        
    def encode(self) -> str:
        """[summary]

        Returns:
            str: [description]
        """
        return f"{self.status.value},{self.message},{self.timestamp}"


class DriverType(Enum):
    INIT = "init"
    RETRY = "retry"


@dataclass
class DriverMessage:
    """[summary]
    """
    status: DriverType
    message: str
    retry_count: int
    driver_timestamp: str = timestamp()

    @classmethod
    def decode_list(cls, message_list: List[str]):
        """
        """
        return list(map(lambda m: DriverMessage.decode(m), message_list))
        
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
        raw_status = ss[0]
        if raw_status == DriverType.INIT.value:
            status = DriverType.INIT
        elif raw_status == DriverType.RETRY.value:
            status = DriverType.RETRY
        else:
            raise Exception(f"DriverMessage parse error: {message_str}")
        message = ss[1]
        retry_count = int(ss[2])
        driver_timestamp = ss[3]

        return DriverMessage(
            status=status,
            message=message,
            retry_count=retry_count,
            driver_timestamp=driver_timestamp
        )
        
    def encode(self) -> str:
        """[summary]

        Returns:
            str: [description]
        """
        return f"{self.status.value},{self.message},{self.retry_count},{self.driver_timestamp}"


    @classmethod
    def convert_from_executor_message(cls, executor_message, status:DriverType = None):
        if status is None:
            status = self.status
        def __convert(executor_message):
            return DriverMessage(
                status = status,
                message = executor_message.message,
                retry_count = executor_message.retry_count,
                driver_timestamp = executor_message.driver_timestamp
            )
        return __convert(executor_message)

@dataclass
class ExecutorMessage:
    """[summary]
    """
    status: DriverType
    message: str
    retry_count: int
    driver_timestamp: str
    executor_id: int
    executor_timestamp: str = timestamp()

    @classmethod
    def decode_list(cls, message_list: List[str]):
        """
        """
        return list(map(lambda m: ExecutorMessage.decode(m), message_list))
        
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
        raw_status = ss[0]
        if raw_status == DriverType.INIT.value:
            status = DriverType.INIT
        elif raw_status == DriverType.RETRY.value:
            status = DriverType.RETRY
        else:
            raise Exception(f"ExecutorMessage parse error: {message_str}")
        message = ss[1]
        retry_count = int(ss[2])
        driver_timestamp = ss[3]
        executor_id = ss[4]
        executor_timestamp = ss[5]

        return ExecutorMessage(
            status=status,
            message=message,
            retry_count=retry_count,
            driver_timestamp=driver_timestamp,
            executor_id=executor_id,
            executor_timestamp=executor_timestamp
        )
        
    def encode(self) -> str:
        """[summary]

        Returns:
            str: [description]
        """
        return f"{self.status.value},{self.message},{self.retry_count},{self.driver_timestamp},{self.executor_id},{self.executor_timestamp}"

    @classmethod
    def encode_list(cls, message_list: List[str]) -> str:
            """[summary]

            Returns:
                str: [description]
            """
            return list(map(lambda m: m.encode(), message_list))
    
    @classmethod
    def convert_from_driver_message_list(cls, executor_id:int, driver_message_list = List[DriverMessage]):
        def __convert(driver_message: DriverMessage):
            return ExecutorMessage(
                status = driver_message.status,
                message = driver_message.message,
                retry_count = driver_message.retry_count + 1,
                driver_timestamp = driver_message.driver_timestamp,
                executor_id = executor_id
            )
        r = list(map(lambda d: __convert(d), driver_message_list))
        return r