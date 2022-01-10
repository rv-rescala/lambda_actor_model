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


def actor_starter(bucket: str, prefix: str, actor_conf: str, trigger_name:str) -> str:
    """[Send task message]

    Args:
        bucket (str): [description]
        prefix (str): [description]
        actor_conf_file (str): [description]
    """
    # init
    s3_client = boto3.client('s3')
    trigger_file = f"{trigger_name}.csv"
    trigger_input_path = f"{actor_conf.trigger_file_prefix}/{trigger_file}"
    tmp_trigger_input_path = f"/tmp/{trigger_file}"

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
            executor_task_message_list = ExecutorTaskMessage.create_message(executor_message_list, trigger_name)
            logger.info(f"executor_task_message_list: {executor_task_message_list}")
            msg_list = send_executor_task_message(queue=executor_task_q, message_body_list=ExecutorTaskMessage.encode_list(executor_task_message_list), task_groupid=trigger_name)
    return msg_list

def executor_init_start(actor_conf: ActorConf, executor_trigger_q, trigger_name:str):
    logger.info(f"executor_init_start")
    logger.info(f"executor_concurrency: {actor_conf.executor_concurrency}")
    for executor_id in range(actor_conf.executor_concurrency):
        task_groupid = trigger_name
        trigger_groupid = f"{task_groupid}_{executor_id}"
        m = ExecutorTriggerMessage(status=ExecutorTriggerStatusType.INIT_START, message=f"executor {executor_id} start", task_groupid=task_groupid, trigger_groupid=trigger_groupid, executor_start_timestamp=timestamp())
        send_execute_trigger_message(queue=executor_trigger_q, message_body=m.encode(), trigger_groupid=trigger_groupid)
        time.sleep(0.5) # Wait

def executor_start(executor_trigger_q, driver_trigger_message: DriverTriggerMessage = None):
    logger.info(f"executor_start")
    logger.info(f"trigger_groupid: {driver_trigger_message.trigger_groupid}")
    m = ExecutorTriggerMessage(status=ExecutorTriggerStatusType.INIT_START, message=f"trigger_groupid {driver_trigger_message.trigger_groupid} start", task_groupid=driver_trigger_message.task_groupid, trigger_groupid=driver_trigger_message.trigger_groupid, executor_start_timestamp=timestamp())
    send_execute_trigger_message(queue=executor_trigger_q, message_body=m.encode(), trigger_groupid=driver_trigger_message.trigger_groupid)

def actor_driver_starter(bucket: str, prefix: str, actor_conf_file: str, trigger_name: str, driver_trigger_message_str: str = None):
    """[summary]

    Args:
        bucket (str): [description]
        prefix (str): [description]
        conf_filename (str): [description]
    """
    # init
    s3_client = boto3.client('s3')
    actor_conf = ActorConf.get_actor_conf(s3_client=s3_client, bucket=bucket, prefix=prefix, actor_conf_file=actor_conf_file)
    logger.info(f"actor_driver: actor driver start, {actor_conf}")

    # sqs init
    sqs = boto3.resource('sqs', region_name='ap-northeast-1')
    driver_trigger_q = sqs.get_queue_by_name(QueueName=actor_conf.driver_trigger_q)
    executor_trigger_q = sqs.get_queue_by_name(QueueName=actor_conf.executor_trigger_q)
    executor_task_q = sqs.get_queue_by_name(QueueName=actor_conf.executor_task_q)

    actor_starter(bucket=bucket, prefix=prefix, actor_conf=actor_conf, trigger_name=trigger_name)
    executor_init_start(actor_conf=actor_conf, executor_trigger_q=executor_trigger_q, trigger_name=trigger_name)


def actor_driver(bucket: str, prefix: str, actor_conf_file: str, driver_trigger_message_str: str):
    """[summary]

    Args:
        bucket (str): [description]
        prefix (str): [description]
        conf_filename (str): [description]
    """
    # init
    s3_client = boto3.client('s3')
    actor_conf = ActorConf.get_actor_conf(s3_client=s3_client, bucket=bucket, prefix=prefix, actor_conf_file=actor_conf_file)
    logger.info(f"actor_driver: actor driver start, {actor_conf}")

    # sqs init
    sqs = boto3.resource('sqs', region_name='ap-northeast-1')
    driver_trigger_q = sqs.get_queue_by_name(QueueName=actor_conf.driver_trigger_q)
    executor_trigger_q = sqs.get_queue_by_name(QueueName=actor_conf.executor_trigger_q)
    executor_task_q = sqs.get_queue_by_name(QueueName=actor_conf.executor_task_q)

    driver_trigger_message = DriverTriggerMessage.decode(driver_trigger_message_str)
    task_q_size = get_q_current_size(qname=actor_conf.executor_task_q)
    print(f"task q count: {task_q_size}")
    if (driver_trigger_message.status == DriverTriggerStatusType.CONTINUE) or (task_q_size > 0):
        print(f"contine: {driver_trigger_message}")
        executor_start(executor_trigger_q=executor_trigger_q, driver_trigger_message=driver_trigger_message)
    elif driver_trigger_message.status == DriverTriggerStatusType.FINISH:
        print(f"actor_driver finish: {driver_trigger_message}")
    else:
        raise ActorDriverException(f"driver_status_message.status  is illegal: {driver_status_message.status}")
    
