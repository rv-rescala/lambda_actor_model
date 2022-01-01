from lambda_actor.actor_executor import actor_executor


def execution_func(message: str) -> str:
    #raise Exception("test")
    return f"hello: {message}"

actor_executor("rescala-configuration", "lambda_actor", "sample_actor.json", execution_func)