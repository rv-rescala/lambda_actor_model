import json
import boto3
from datetime import datetime
import time
from lambda_actor.aws.sqs import *
import logging
from lambda_actor.types.type_conf import ActorConf
from lambda_actor.types.type_actor_message import *

logger = logging.getLogger()

def clear_all_q(bucket: str, prefix: str, actor_conf_file: str):
    """
    """
    s3_client = boto3.client('s3')
    actor_conf = ActorConf.get_actor_conf(s3_client=s3_client, bucket=bucket, prefix=prefix, actor_conf_file=actor_conf_file)

    logger.info(f"actor_conf: {actor_conf}")

    sqs = boto3.resource('sqs', region_name='ap-northeast-1')
    driver_trigger_q = sqs.get_queue_by_name(QueueName=actor_conf.driver_trigger_q)
    executor_trigger_q = sqs.get_queue_by_name(QueueName=actor_conf.executor_trigger_q)
    executor_task_q = sqs.get_queue_by_name(QueueName=actor_conf.executor_task_q)

    logger.info(f"clear driver_trigger_q: {receive_all(driver_trigger_q)}")
    logger.info(f"clear executor_trigger_q: {receive_all(executor_trigger_q)}")
    logger.info(f"clear executor_task_q: {receive_all(executor_task_q)}")