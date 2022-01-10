from lambda_actor.actor_driver import *
from lambda_actor.actor_executor import *
import time
import boto3
from lambda_actor.clear_sqs import clear_all_q

def execution_func(message):
    #raise Exception("test")
    print(f"execution_func: {message}")
    time.sleep(35)
    raise Exception("execution_func error")
    return f"executed: {message}"

def success_func(message):
    #raise Exception("test")
    print(f"success_func: {message}")
    return f"successed: {message}"

def failed_func(message):
    #raise Exception("test")
    print(f"failed_func: {message}")
    return f"failed: {message}"

driver_trigger_message_str = """{
    "status": "continue",
    "message": "trigger_groupid was unext_comiklist_0",
    "trigger_groupid": "unext_comiklist_0",
    "task_groupid": "unext_comiklist",
    "driver_start_timestamp": "2022/01/10 16:57:24"
}"""

clear_all_q("captool", "conf", "actor_conf.json")
actor_driver_starter(bucket="captool", prefix="conf", actor_conf_file="actor_conf.json", trigger_name="unext_comiklist")

for i in range(3):
    actor_executor(bucket="captool", prefix="conf", actor_conf_file="actor_conf.json", execution_func=execution_func, success_func=success_func, failed_func=failed_func)
    actor_driver(bucket="captool", prefix="conf", actor_conf_file="actor_conf.json", driver_trigger_message_str=driver_trigger_message_str)

#clear_all_q("captool", "conf", "actor_conf.json")