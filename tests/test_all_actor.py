from lambda_actor.actor_driver import *
from lambda_actor.actor_starter import *
from lambda_actor.actor_executor import *
import time
import boto3

def execution_func(message: str) -> str:
    #raise Exception("test")
    return f"hello: {message}"

def finally_func(message_list) -> str:
    if len(message_list) != 0:
        executor_id = message_list[0].executor_id
        s3_client = boto3.client('s3')
        s = "\n".join(list(map(lambda x: x.result, message_list)))
        with open("/tmp/actor_test.csv", "w") as f:
            f.write(s)
        s3_client.upload_file("/tmp/actor_test.csv", "tmpout", f"actor/actor_test_{executor_id}.csv")

actor_starter(bucket="rescala-configuration", prefix="lambda_actor", filename="sample_actor.json")
actor_driver(bucket="rescala-configuration", prefix="lambda_actor", filename="sample_actor.json")
actor_executor(bucket="rescala-configuration", prefix="lambda_actor", filename="sample_actor.json", execution_func=execution_func, finally_func=finally_func)
actor_driver(bucket="rescala-configuration", prefix="lambda_actor", filename="sample_actor.json")
