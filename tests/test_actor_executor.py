from lambda_actor.actor_executor import actor_executor
import time

def execution_func(message: str) -> str:
    #raise Exception("test")
    print(message)
    return f"hello: {message}"

# bucket: str, prefix: str, actor_conf_file: str, execution_func, finally_func = None, executor_trigger_message_str: str = None
actor_executor(bucket="captool", prefix="conf", actor_conf_file="actor_conf.json", execution_func=execution_func)