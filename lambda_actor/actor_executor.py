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
from lambda_actor.utils.dateutil import timestamp

logger = logging.getLogger()


class ActorExecutorException(Exception):
    pass

def actor_executor(bucket: str, prefix: str, conf_filename: str, execution_func, finally_func = None, executor_trigger_message_str: str = None):
    """[summary]

    Args:
        bucket (str): [description]
        prefix (str): [description]
        conf_filename (str): [description]
    """
    # init
    s3_client = boto3.client('s3')
    actor_conf = ActorConf.get_actor_conf(s3_client=s3_client, bucket=bucket, prefix=prefix, conf_filename=conf_filename)
    logger.info(f"actor_conf: {actor_conf}")

    # sqs init
    sqs = boto3.resource('sqs', region_name='ap-northeast-1')
    driver_trigger_q = sqs.get_queue_by_name(QueueName=actor_conf.driver_trigger_q)
    executor_trigger_q = sqs.get_queue_by_name(QueueName=actor_conf.executor_trigger_q)
    executor_task_q = sqs.get_queue_by_name(QueueName=actor_conf.executor_task_q)

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
    executor_result_list = []
    driver_trigger_status = DriverTriggerStatusType.CONTINUE

    while True:
        executer_task_message_str = receive(executor_task_q, 1)

        # Check task size
        if len(executer_task_message_str) == 0:
            logger.info(f"executor_task_q is zero, {executor_trigger_message}")
            driver_trigger_status = DriverTriggerStatusType.FINISH
            break

        executor_task_message = ExecutorTaskMessage.decode(executer_task_message_str[0])

        # run func
        task_start = time.time()
        executor_result_status = None
        result = None
        try:
            result = execution_func(executor_task_message.message)
            executor_result_status = ExecutorResultStatusType.SUCCESS
        except Exception as e:
            # when the task failed, retry
            retry_count = executor_task_message.retry_count + 1
            if retry_count > actor_conf.max_retry:
                logger.error(f"actor_executor: {executor_task_message} is failed, {e}")
                result = str(e)
                executor_result_status = ExecutorResultStatusType.FAILED
            else:
                logger.error(f"actor_executor: {retry_executor_task_message} is retried, {e}")
                executor_result_status = ExecutorResultStatusType.FAILED
                retry_executor_task_message = ExecutorTaskMessage(
                    message=executor_task_message.message,
                    retry_count=retry_count,
                    driver_start_timestamp=executor_task_message.driver_start_timestamp
                )
                retry_message = ExecutorTaskMessage.encode(retry_executor_task_message)
                send_executor_task_message(executor_task_q, [retry_message])
        # timer
        task_end = time.time()
        executed_time = task_end - executor_start
        task_time = task_end - task_start

        if result:
            # Create result
            executor_result = ExecutorResultMessage(
                status = executor_result_status,
                result = result,
                driver_start_timestamp = executor_task_message.driver_start_timestamp,
                executor_start_timestamp = executor_trigger_message.executor_trigger_timestamp,
                retry_count = executor_task_message.retry_count,
                executor_id = executor_trigger_message.executor_id,
                execute_time = task_time,
                timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            )
            executor_result_list.append(executor_result)
        if executed_time > timeout:
            logger.info(f"actor_executor is timeout, {executor_trigger_message}")
            break
        else:
            logger.debug(executed_time)

    # run own func
    if finally_func:
        finally_func(executor_result_list)

    # send finish message
    if driver_trigger_status == DriverTriggerStatusType.CONTINUE:
        retrun_driver_trigger_message = DriverTriggerMessage(
                status=DriverTriggerStatusType.CONTINUE,
                message=f"executor_id was {executor_trigger_message.executor_id}",
                executor_id=executor_trigger_message.executor_id,
                driver_trigger_timestamp = timestamp()
        )
    elif driver_trigger_status == DriverTriggerStatusType.FINISH:
        retrun_driver_trigger_message = DriverTriggerMessage(
                status=DriverTriggerStatusType.FINISH,
                message=f"executor_id was {executor_trigger_message.executor_id}",
                executor_id=executor_trigger_message.executor_id,
                driver_trigger_timestamp = timestamp()
        )
    else:
        raise ActorExecutorException("illegal driger status type")
    
    send_driver_trigger_message(queue=driver_trigger_q, message_body=retrun_driver_trigger_message.encode(), executor_id=executor_trigger_message.executor_id, executor_key=actor_conf.executor_key)

    