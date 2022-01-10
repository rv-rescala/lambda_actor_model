from lambda_actor.actor_driver import actor_driver

# actor_driver(bucket: str, prefix: str, actor_conf_file: str, trigger_file: str, driver_trigger_message_str: str = None)
r = actor_driver(bucket="captool", prefix="conf", actor_conf_file="actor_conf.json", trigger_name="unext_comiklist")
print(r)