import json
import boto3
from datetime import datetime
import time
from lambda_actor.aws.sqs import *
import logging
from lambda_actor.types.type_conf import ActorConf
from lambda_actor.types.type_actor_message import *
from typing import List
import time

logger = logging.getLogger()


class ActorExecutorException(Exception):
    pass

def actor_executor(bucket: str, prefix: str, filename: str, execution_func, executor_trigger_message_str: str = None):
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
    if executor_trigger_message_str is None:
        executor_trigger_message_str = receive(executor_trigger_q, 1)
        logger.info(executor_trigger_message_str)
        if len(executor_trigger_message_str) == 0:
            raise ActorExecutorException("executor_trigger_message_str is zero")
        executor_trigger_message = ExecutorTriggerMessage.decode(executor_trigger_message_str[0])
    else:
        executor_trigger_message = ExecutorTriggerMessage.decode(executor_trigger_message_str)

    # timer
    timeout = actor_conf.max_lambda_execution_time
    logger.info(f"executor timeout: {timeout}")
    executor_start = time.time()

    while True:
        executer_task_message_str = receive(executor_task_q, 1)
        # Check task size
        if len(executer_task_message_str) == 0:
            logger.info(f"executor_task_q is zero, {executor_trigger_message}")
            break
        executor_task_message = ExecutorTaskMessage.decode(executer_task_message_str[0])

        # run func
        task_start = time.time()
        status = None
        try:
            result_message = execution_func(executor_task_message.message)
            status = ExecutorResultStatusType.SUCCESS
        except Exception as e:
            # when the task failed, retry
            logger.error(e)
            retry_count = executor_task_message.retry_count + 1
            if retry_count > actor_conf.max_retry:
                result_message = ""
                status = ExecutorResultStatusType.FAILED
                logger.error(f"actor_executor: {executor_task_message} is failed")
            else:
                retry_executor_task_message = ExecutorTaskMessage(
                    message=executor_task_message.message,
                    retry_count=retry_count
                )
                retry_message = ExecutorTaskMessage.encode(retry_executor_task_message)
                print(retry_message)
                send(executor_task_q, [retry_message])
                logger.warn(f"actor_executor: {retry_executor_task_message} is retried")

        # timer
        task_end = time.time()
        executed_time = task_end - executor_start
        task_time = task_end - task_start

        if status:
            # Create result
            executor_result_message = ExecutorResultMessage(
                status = status,
                message = result_message,
                driver_trigger_timestamp = executor_trigger_message.driver_trigger_timestamp,
                executor_trigger_timestamp = executor_trigger_message.executor_trigger_timestamp,
                retry_count = executor_task_message.retry_count,
                executor_id = executor_trigger_message.executor_id,
                execute_time = task_time
            )
            send(executor_result_q, [ExecutorResultMessage.encode(executor_result_message)])
        if executed_time > timeout:
            logger.info(f"actor_executor is timeout, {executor_trigger_message}")
            break
        else:
            logger.debug(executed_time)
    
    retrun_driver_trigger_message = DriverTriggerMessage(
            status=DriverTriggerStatusType.EXECUTOR_FINISH,
            message=f"executor_id was {executor_trigger_message.executor_id}"
    )
    send(driver_trigger_q, [DriverTriggerMessage.encode(retrun_driver_trigger_message)])