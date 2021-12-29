import boto3
import logging
import traceback
from mangawalk_actor.types.type_conf import ActorConf
from mangawalk_actor.types.type_actor_message import DriverStatusType, DriverStatusMessage, DriverType, DriverMessage
from mangawalk_actor.awslambda.sqs import enqueue

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
    driver_fifo = sqs.get_queue_by_name(QueueName=actor_conf.driver_fifo)
    driver_status_fifo = sqs.get_queue_by_name(QueueName=actor_conf.driver_status_fifo)
    
    # get csv
    s3_client.download_file(actor_conf.trigger_file_bucket, trigger_input_path, tmp_trigger_input_path)
    with open(tmp_trigger_input_path) as f:
            url_list = f.read().split('\n')[1:] #Skip header
            logger.info(f"url_list: {url_list}")
            
            # add driver_fifo queue
            # driver_fifoに初めにエンキューする必要がある
            driver_message = list(map(lambda x: DriverMessage(status=DriverType.INIT, message=x, retry_count=0).encode(), url_list))
            logger.info(f"driver_message: {driver_message}")
            enqueue(driver_fifo, driver_message)

            # add driver_status_fifo queue
            driver_status_message = [DriverStatusMessage(status=DriverStatusType.INIT, message="init").encode()]
            print(f"driver_status_message: {driver_status_message}")
            enqueue(driver_status_fifo, driver_status_message)

