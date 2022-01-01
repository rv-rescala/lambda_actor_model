from lambda_actor.actor_driver import actor_driver

r = actor_driver("rescala-configuration", "lambda_actor", "sample_actor.json")
print(r)