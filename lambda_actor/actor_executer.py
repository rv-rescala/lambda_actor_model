import json
import boto3
from datetime import datetime
import time
from mangawalk_actor.awslambda.sqs import enqueue, dequeue
import logging
from mangawalk_actor.types.type_conf import ActorConf
from mangawalk_actor.types.type_actor_message import DriverStatusType, DriverStatusMessage, DriverType, DriverMessage, ExecutorMessage
from typing import List


logger = logging.getLogger()


class ActorExecutorException(Exception):
    pass

def actor_executer(bucket: str, prefix: str, filename: str, executor_message_str_list: List[str] = None):
    """[summary]

    Args:
        bucket (str): [description]
        prefix (str): [description]
        filename (str): [description]
    """
    # init
    s3_client = boto3.client('s3')
    actor_conf = ActorConf.get_actor_conf(s3_client=s3_client, bucket=bucket, prefix=prefix, filename=filename)
    trigger_input_path = f"{actor_conf.trigger_file_prefix}/{actor_conf.trigger_file}"
    tmp_trigger_input_path = f"/tmp/{actor_conf.trigger_file}"
    logger.info(f"trigger_input_path: {trigger_input_path}")
    logger.info(f"tmp_trigger_input_path: {tmp_trigger_input_path}")
    logger.info(f"actor_conf: {actor_conf}")

    # sqs init
    sqs = boto3.resource('sqs', region_name='ap-northeast-1')
    driver_fifo = sqs.get_queue_by_name(QueueName=actor_conf.driver_fifo)
    driver_status_fifo = sqs.get_queue_by_name(QueueName=actor_conf.driver_status_fifo)

    if executor_message_str_list is None:
        executor_fifo = sqs.get_queue_by_name(QueueName=actor_conf.executor_fifo)
        executor_message_str_list = dequeue(executor_fifo, 10)
        print(executor_message_str_list)

    if len(executor_message_str_list) == 0:
        m = f"actor_executer message is zero"
        logger.error(m)
        raise ActorExecutorException(m)
    
    executor_message_list = ExecutorMessage.decode_list(executor_message_str_list)
    executor_id = executor_message_list[0].executor_id

    def __executor(executor_message):
        try:
            # write your function
            executor_message = DriverMessage.convert_from_executor_message(executor_message=executor_message, status=DriverType.RETRY)
            print(executor_message)
            return None
        except:
            m = f"actor_executer error: {executor_message}"
            logger.error(m)
            executor_message = DriverMessage.convert_from_executor_message(executor_message=executor_message, status=DriverType.RETRY)
            return m

    list(map(lambda x: __executor(x), executor_message_list))

    driver_status_message = [DriverStatusMessage(status=DriverStatusType.FINISH, message=executor_id).encode()]
    print(f"driver_status_message: {driver_status_message}")
    enqueue(driver_status_fifo, driver_status_message)