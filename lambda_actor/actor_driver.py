import json
import boto3
from datetime import datetime
import time
from mangawalk_actor.awslambda.sqs import enqueue, dequeue
import logging
from mangawalk_actor.types.type_conf import ActorConf
from mangawalk_actor.types.type_actor_message import DriverStatusType, DriverStatusMessage, DriverType, DriverMessage, ExecutorMessage


logger = logging.getLogger()

class ActorDriverException(Exception):
    pass

def executor_init_start(executor_concurrency: int, driver_fifo, executor_fifo):
    print(f"executor_init_start")
    print(f"executor_concurrency: {executor_concurrency}")

    for executor_id in range(3):
        executor_start(executor_id, driver_fifo, executor_fifo)


def executor_start(executor_id: int, driver_fifo, executor_fifo):
    print(f"executor_start")
    print(f"executor_id: {executor_id}")

    r = dequeue(driver_fifo, 10)
    if len(r) == 0:
        logger.error(f"driver_fifo is zero: {executor_id}")
        raise Exception(f"driver_fifo is zero: {executor_id}")

    driver_message_list = DriverMessage.decode_list(r)
    executor_message_list = ExecutorMessage.convert_from_driver_message_list(executor_id=executor_id, driver_message_list = driver_message_list)
    executor_message =  ExecutorMessage.encode_list(executor_message_list)
    logger.debug(f"executor_message: {executor_message}")
    enqueue(executor_fifo, executor_message)

def actor_driver(bucket: str, prefix: str, filename: str, driver_status_message: str = None):
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
    executor_fifo = sqs.get_queue_by_name(QueueName=actor_conf.executor_fifo)

    if driver_status_message is None:
        driver_status_fifo = sqs.get_queue_by_name(QueueName=actor_conf.driver_status_fifo)
        driver_status_message_list = dequeue(driver_status_fifo)
        print(driver_status_message_list)
        if len(driver_status_message_list) == 0:
            raise ActorDriverException("driver_status_message is zero")
        driver_status_message = DriverStatusMessage.decode(driver_status_message_list[0])

    if driver_status_message.status == DriverStatusType.INIT:
        executor_init_start(executor_concurrency=actor_conf.executor_concurrency, driver_fifo=driver_fifo, executor_fifo=executor_fifo)
    elif driver_status_message.status == DriverStatusType.FINISH:
        executor_id = driver_status_message.message
        logger.info(f"actor_driver: DriverStatusType.FINISH, executor_id is {executor_id}")
        executor_start(executor_id=executor_id, driver_fifo=driver_fifo, executor_fifo=executor_fifo)
    else:
        raise ActorDriverException(f"driver_status_message.status  is illegal: {driver_status_message.status}")


    
