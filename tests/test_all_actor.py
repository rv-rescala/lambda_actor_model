from lambda_actor.actor_driver import *
from lambda_actor.actor_starter import *
from lambda_actor.actor_executor import *
import time

def execution_func(message: str) -> str:
    #raise Exception("test")
    return f"hello: {message}"

actor_starter("rescala-configuration", "lambda_actor", "sample_actor.json")
actor_driver("rescala-configuration", "lambda_actor", "sample_actor.json")
actor_executor("rescala-configuration", "lambda_actor", "sample_actor.json", execution_func)
actor_driver("rescala-configuration", "lambda_actor", "sample_actor.json")
