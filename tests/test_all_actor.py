from lambda_actor.actor_driver import *
from lambda_actor.actor_executor import *
import time
import boto3
from lambda_actor.clear_sqs import clear_all_q

def execution_func(message: str) -> str:
    #raise Exception("test")
    #time.sleep(1)
    #print(message)
    return f"hello: {message}"


def execution_func_finish(message: str) -> str:
    #raise Exception("test")
    #print(message)
    return f"hello_finish: {message}"

"""
def finally_func(message_list) -> str:
    if len(message_list) != 0:
        executor_id = message_list[0].executor_id
        s3_client = boto3.client('s3')
        s = "\n".join(list(map(lambda x: x.result, message_list)))
        with open("/tmp/actor_test.csv", "w") as f:
            f.write(s)
        s3_client.upload_file("/tmp/actor_test.csv", "tmpout", f"actor/actor_test_{executor_id}.csv")
"""
finally_func=None

driver_trigger_message_str = """{
    "status": "continue",
    "message": "executor_id was 0",
    "executor_id": 0,
    "driver_trigger_timestamp": "2022/01/01 11:35:19"
}"""


driver_trigger_message_finish_str = """{
    "status": "finish",
    "message": "executor_id was 0",
    "executor_id": 0,
    "driver_trigger_timestamp": "2022/01/01 11:35:19"
}"""

clear_all_q("rescala-configuration", "lambda_actor", "sample_actor.json")
actor_driver(bucket="rescala-configuration", prefix="lambda_actor", conf_filename="sample_actor.json")
actor_executor(bucket="rescala-configuration", prefix="lambda_actor", conf_filename="sample_actor.json", execution_func=execution_func, finally_func=finally_func)
actor_driver(bucket="rescala-configuration", prefix="lambda_actor", conf_filename="sample_actor.json", driver_trigger_message_str=driver_trigger_message_str)
actor_executor(bucket="rescala-configuration", prefix="lambda_actor", conf_filename="sample_actor.json", execution_func=execution_func_finish, finally_func=finally_func)
actor_driver(bucket="rescala-configuration", prefix="lambda_actor", conf_filename="sample_actor.json", driver_trigger_message_str=driver_trigger_message_finish_str)