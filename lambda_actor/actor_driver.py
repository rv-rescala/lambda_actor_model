import json
import boto3
from datetime import datetime
import time
from lambda_actor.aws.sqs import *
import logging
from lambda_actor.types.type_conf import ActorConf
from lambda_actor.types.type_actor_message import *


logger = logging.getLogger()

class ActorDriverException(Exception):
    pass

def executor_init_start(executor_concurrency: int, driver_trigger_message, executor_trigger_q):
    logger.info(f"executor_init_start")
    logger.info(f"executor_concurrency: {executor_concurrency}")

    for executor_id in range(executor_concurrency):
        executor_start(executor_id=executor_id, driver_trigger_message=driver_trigger_message, executor_trigger_q=executor_trigger_q)


def executor_start(executor_id: int, driver_trigger_message, executor_trigger_q):
    logger.info(f"executor_start")
    logger.info(f"executor_id: {executor_id}")
    m = ExecutorTriggerMessage(status=ExecutorTriggerStatusType.START, message=f"executor {executor_id} start", driver_trigger_timestamp=driver_trigger_message.driver_trigger_timestamp, executor_id=executor_id)
    send(executor_trigger_q, [m.encode()])

def actor_driver(bucket: str, prefix: str, filename: str, driver_trigger_message: str = None):
    """[summary]

    Args:
        bucket (str): [description]
        prefix (str): [description]
        filename (str): [description]
    """
    # init
    s3_client = boto3.client('s3')
    actor_conf = ActorConf.get_actor_conf(s3_client=s3_client, bucket=bucket, prefix=prefix, filename=filename)
    logger.info(f"actor_conf: {actor_conf}")

    # sqs init
    sqs = boto3.resource('sqs', region_name='ap-northeast-1')
    driver_trigger_q = sqs.get_queue_by_name(QueueName=actor_conf.driver_trigger_q)
    executor_trigger_q = sqs.get_queue_by_name(QueueName=actor_conf.executor_trigger_q)
    executor_task_q = sqs.get_queue_by_name(QueueName=actor_conf.executor_task_q)
    executor_result_q = sqs.get_queue_by_name(QueueName=actor_conf.executor_result_q)

    # Run mannualy
    if driver_trigger_message is None:
        driver_trigger_message_list = receive(queue=driver_trigger_q, fetch_count=1)
        logger.info(driver_trigger_message_list)
        if len(driver_trigger_message_list) == 0:
            raise ActorDriverException("driver_trigger_message_list is zero")
        driver_trigger_message = DriverTriggerMessage.decode(driver_trigger_message_list[0])
    else:
        driver_trigger_message = DriverTriggerMessage.decode(driver_trigger_message)

    if driver_trigger_message.status == DriverTriggerStatusType.INIT:
        executor_init_start(executor_concurrency=actor_conf.executor_concurrency, driver_trigger_message=driver_trigger_message, executor_trigger_q=executor_trigger_q)
    elif driver_trigger_message.status == DriverTriggerStatusType.EXECUTOR_FINISH:
        logger.info(f"actor_driver: DriverStatusType.FINISH, executor_id is {driver_trigger_message}")
        results = receive_all(executor_result_q)
        print(results)
    else:
        raise ActorDriverException(f"driver_status_message.status  is illegal: {driver_status_message.status}")
    


    
