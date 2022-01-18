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
import traceback

logger = logging.getLogger()


class ActorExecutorException(Exception):
    pass

def actor_executor(bucket: str, prefix: str, actor_conf_file: str, execution_func, success_func = None, failed_func = None, executor_trigger_message_str: str = None):
    """[summary]

    Args:
        bucket (str): [description]
        prefix (str): [description]
        conf_filename (str): [description]
    """
    # init
    s3_client = boto3.client('s3')
    actor_conf = ActorConf.get_actor_conf(s3_client=s3_client, bucket=bucket, prefix=prefix, actor_conf_file=actor_conf_file)
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
            result = execution_func(executor_task_message)
            executor_result_status = ExecutorResultStatusType.SUCCESS
        except Exception as e:
            # when the task failed, retry
            error_message = str(e)
            stack_trace = traceback.format_exc().replace("\n", " ")
            retry_count = executor_task_message.retry_count + 1
            if retry_count > actor_conf.max_retry:
                executor_result_status = ExecutorResultStatusType.FAILED
                logger.error(f'actor_executor is failed, {executor_task_message},{error_message},{stack_trace}')
            else:
                executor_result_status = ExecutorResultStatusType.FAILED
                retry_executor_task_message = ExecutorTaskMessage(
                    message=executor_task_message.message,
                    retry_count=retry_count,
                    task_groupid=executor_task_message.task_groupid,
                    driver_start_timestamp=executor_task_message.driver_start_timestamp
                )
                retry_message = ExecutorTaskMessage.encode(retry_executor_task_message)
                send_executor_task_message(queue=executor_task_q, message_body_list=[retry_message], task_groupid=executor_trigger_message.task_groupid)
                logger.warning(f'actor_executor is retried, {retry_executor_task_message}, {error_message}, {stack_trace}')

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
                executor_start_timestamp = executor_trigger_message.executor_start_timestamp,
                retry_count = executor_task_message.retry_count,
                trigger_groupid = executor_trigger_message.trigger_groupid,
                task_groupid = executor_trigger_message.task_groupid,
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
    #
    success_list = list(filter(lambda x: x.status != ExecutorResultStatusType.FAILED, executor_result_list))
    failed_list = list(filter(lambda x: x.status == ExecutorResultStatusType.FAILED, executor_result_list))
    
    if success_func:
        if len(success_list) > 0:
            success_func(success_list)
        else:
            logger.info("success_list is zero")
    if failed_func:
        if len(failed_list) > 0:
            failed_func(failed_list)
        else:
            logger.info("failed_list is zero")

    # send finish message
    if driver_trigger_status == DriverTriggerStatusType.CONTINUE:
        retrun_driver_trigger_message = DriverTriggerMessage(
                status=DriverTriggerStatusType.CONTINUE,
                message=f"trigger_groupid was {executor_trigger_message.trigger_groupid}",
                trigger_groupid = executor_trigger_message.trigger_groupid,
                task_groupid = executor_trigger_message.task_groupid,
                driver_start_timestamp = timestamp()
        )
    elif driver_trigger_status == DriverTriggerStatusType.FINISH:
        retrun_driver_trigger_message = DriverTriggerMessage(
                status=DriverTriggerStatusType.FINISH,
                message=f"trigger_groupid was {executor_trigger_message.trigger_groupid}",
                trigger_groupid = executor_trigger_message.trigger_groupid,
                task_groupid = executor_trigger_message.task_groupid,
                driver_start_timestamp = timestamp()
        )
    else:
        raise ActorExecutorException("illegal driger status type")
    
    send_driver_trigger_message(queue=driver_trigger_q, message_body=retrun_driver_trigger_message.encode(), trigger_groupid=executor_trigger_message.trigger_groupid)

    