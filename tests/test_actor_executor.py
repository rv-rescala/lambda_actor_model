from lambda_actor.actor_executor import actor_executor
import time

def execution_func(message: str) -> str:
    time.sleep(1)
    return f"hello: {message}"

actor_executor("rescala-configuration", "lambda_actor", "sample_actor.json",execution_func,"start,executor 0 start,2021/12/30 16:32:50,0,2021/12/30 16:31:19")
