import json
import boto3
from datetime import datetime
import time
from lambda_actor.aws.sqs import *
import logging
from lambda_actor.types.type_conf import ActorConf
from lambda_actor.types.type_actor_message import *
from lambda_actor.utils.dateutil import timestamp


logger = logging.getLogger()

class ActorDriverException(Exception):
    pass

def actor_starter(bucket: str, prefix: str, conf_filename: str):
    """[Send task message]

    Args:
        bucket (str): [description]
        prefix (str): [description]
        conf_filename (str): [description]
    """
    # init
    s3_client = boto3.client('s3')
    actor_conf = ActorConf.get_actor_conf(s3_client=s3_client, bucket=bucket, prefix=prefix, conf_filename=conf_filename)
    trigger_input_path = f"{actor_conf.trigger_file_prefix}/{actor_conf.trigger_file}"
    tmp_trigger_input_path = f"/tmp/{actor_conf.trigger_file}"

    # sqs init
    sqs = boto3.resource('sqs', region_name='ap-northeast-1')
    executor_task_q = sqs.get_queue_by_name(QueueName=actor_conf.executor_task_q)
    
    # get csv
    s3_client.download_file(actor_conf.trigger_file_bucket, trigger_input_path, tmp_trigger_input_path)
    with open(tmp_trigger_input_path) as f:
            #executor_message_list = f.read().split('\n')[1:] #Skip header
            executor_message_list = f.read().split('\n')
            logger.info(f"executor_message_list: {executor_message_list}")
            
            # add executor task message
            executor_task_message_list = ExecutorTaskMessage.create_message(executor_message_list)
            logger.info(f"executor_task_message_list: {executor_task_message_list}")
            send_executor_task_message(queue=executor_task_q, message_body_list=ExecutorTaskMessage.encode_list(executor_task_message_list), executor_key=actor_conf.executor_key)

def executor_init_start(executor_concurrency: int, executor_key: str, executor_trigger_q):
    logger.info(f"executor_init_start")
    logger.info(f"executor_concurrency: {executor_concurrency}")
    for executor_id in range(executor_concurrency):
        executor_start(executor_id=executor_id, executor_key=executor_key, executor_trigger_q=executor_trigger_q)

def executor_start(executor_id: int, executor_key: str, executor_trigger_q,  driver_trigger_message: DriverTriggerMessage = None):
    logger.info(f"executor_start")
    logger.info(f"executor_id: {executor_id}")
    if driver_trigger_message:
        m = ExecutorTriggerMessage(status=ExecutorTriggerStatusType.START, message=f"executor {executor_id} start", driver_trigger_timestamp=driver_trigger_message.driver_trigger_timestamp, executor_id=executor_id, executor_trigger_timestamp=timestamp())
    else:
        m = ExecutorTriggerMessage(status=ExecutorTriggerStatusType.INIT_START, message=f"executor {executor_id} start", driver_trigger_timestamp=timestamp(), executor_id=executor_id, executor_trigger_timestamp=timestamp())
    print(m)
    send_execute_message(queue=executor_trigger_q, message_body=m.encode(), executor_id=executor_id, executor_key=executor_key)
    time.sleep(1) # Wait

def actor_driver(bucket: str, prefix: str, conf_filename: str, finally_func=None, driver_trigger_message_str: str = None):
    """[summary]

    Args:
        bucket (str): [description]
        prefix (str): [description]
        conf_filename (str): [description]
    """
    # init
    s3_client = boto3.client('s3')
    actor_conf = ActorConf.get_actor_conf(s3_client=s3_client, bucket=bucket, prefix=prefix, conf_filename=conf_filename)
    logger.info(f"actor_driver: actor driver start, {actor_conf}")

    # sqs init
    sqs = boto3.resource('sqs', region_name='ap-northeast-1')
    driver_trigger_q = sqs.get_queue_by_name(QueueName=actor_conf.driver_trigger_q)
    executor_trigger_q = sqs.get_queue_by_name(QueueName=actor_conf.executor_trigger_q)
    executor_task_q = sqs.get_queue_by_name(QueueName=actor_conf.executor_task_q)

    if driver_trigger_message_str is None:     # Init run
        actor_starter(bucket=bucket, prefix=prefix, conf_filename=conf_filename)
        executor_init_start(executor_concurrency=actor_conf.executor_concurrency, executor_key=actor_conf.executor_key, executor_trigger_q=executor_trigger_q)
    else:
        driver_trigger_message = DriverTriggerMessage.decode(driver_trigger_message_str)
        task_q_size = get_q_current_size(qname=actor_conf.executor_task_q)
        print(f"task q count: {task_q_size}")
        print(task_q_size > 0)
        if (driver_trigger_message.status == DriverTriggerStatusType.CONTINUE) or (task_q_size > 0):
            print(f"contine: {driver_trigger_message}")
            executor_start(executor_id=driver_trigger_message.executor_id, executor_key=actor_conf.executor_key, executor_trigger_q=executor_trigger_q, driver_trigger_message=driver_trigger_message)
        elif driver_trigger_message.status == DriverTriggerStatusType.FINISH:
            print(f"actor_driver: DriverStatusType.FINISH, executor_id is {driver_trigger_message}")
            if finally_func:
                finally_func()
        else:
            raise ActorDriverException(f"driver_status_message.status  is illegal: {driver_status_message.status}")
    


    
