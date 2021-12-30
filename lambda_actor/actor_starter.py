import boto3
import logging
import traceback
from lambda_actor.types.type_conf import ActorConf
from lambda_actor.types.type_actor_message import *
from lambda_actor.aws.sqs import *

logger = logging.getLogger()


def actor_starter(bucket: str, prefix: str, filename: str):
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
    driver_trigger_q = sqs.get_queue_by_name(QueueName=actor_conf.driver_trigger_q)
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
            send(executor_task_q, ExecutorTaskMessage.encode_list(executor_task_message_list))

            # add driver_status_fifo queue
            driver_trigger_message = [DriverTriggerMessage(status=DriverTriggerStatusType.INIT, message="start actor").encode()]
            send(driver_trigger_q, driver_trigger_message)

