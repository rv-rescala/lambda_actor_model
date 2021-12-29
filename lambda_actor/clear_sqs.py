import json
import boto3
from datetime import datetime
import time
from mangawalk_actor.awslambda.sqs import dequeue_all
import logging
from mangawalk_actor.types.type_conf import ActorConf
from mangawalk_actor.types.type_actor_message import DriverStatusType, DriverStatusMessage, DriverType, DriverMessage

logger = logging.getLogger()

def clear_sqs(bucket: str, prefix: str, filename: str):
    s3_client = boto3.client('s3')
    actor_conf = ActorConf.get_actor_conf(s3_client=s3_client, bucket=bucket, prefix=prefix, filename=filename)
    trigger_input_path = f"{actor_conf.trigger_file_prefix}/{actor_conf.trigger_file}"
    tmp_trigger_input_path = f"/tmp/{actor_conf.trigger_file}"
    logger.info(f"trigger_input_path: {trigger_input_path}")
    logger.info(f"tmp_trigger_input_path: {tmp_trigger_input_path}")
    logger.info(f"actor_conf: {actor_conf}")

    sqs = boto3.resource('sqs', region_name='ap-northeast-1')
    driver_fifo = sqs.get_queue_by_name(QueueName=actor_conf.driver_fifo)
    driver_status_fifo = sqs.get_queue_by_name(QueueName=actor_conf.driver_status_fifo)
    executor_fifo = sqs.get_queue_by_name(QueueName=actor_conf.executor_fifo)

    logger.info(f"clear driver_fifo: {dequeue_all(driver_fifo)}")
    logger.info(f"clear driver_status_fifo: {dequeue_all(driver_status_fifo)}")
    logger.info(f"clear executor_fifo: {dequeue_all(executor_fifo)}")