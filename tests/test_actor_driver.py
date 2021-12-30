from lambda_actor.actor_driver import actor_driver

r = actor_driver("rescala-configuration", "lambda_actor", "sample_actor.json", "init,start actor,2021/12/30 16:09:02")
print(r)