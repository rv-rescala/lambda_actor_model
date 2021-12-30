from lambda_actor.actor_driver import *
from lambda_actor.actor_starter import *
from lambda_actor.actor_executor import *
import time
import boto3

def execution_func(message: str) -> str:
    #raise Exception("test")
    return f"hello: {message}"

def finally_func(message_list) -> str:
    s3_client = boto3.client('s3')
    s = "\n".join(list(map(lambda x: x.encode(), message_list)))
    with open("/tmp/actor_test.csv", "w") as f:
        f.write(s)
        s3_client.upload_file("/tmp/actor_test.csv", "tmpout", "actor_test.csv")


actor_starter("rescala-configuration", "lambda_actor", "sample_actor.json")
actor_driver("rescala-configuration", "lambda_actor", "sample_actor.json", finally_func)
actor_executor("rescala-configuration", "lambda_actor", "sample_actor.json", execution_func)
actor_driver("rescala-configuration", "lambda_actor", "sample_actor.json", finally_func)
