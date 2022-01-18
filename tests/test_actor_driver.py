from lambda_actor.actor_driver import actor_driver_starter

# actor_driver(bucket: str, prefix: str, actor_conf_file: str, trigger_file: str, driver_trigger_message_str: str = None)
actor_driver_starter(bucket="captool", prefix="conf", actor_conf_file="actor_conf.json", trigger_name="unext_comiklist")
actor_driver_starter(bucket="captool", prefix="conf", actor_conf_file="actor_conf.json", trigger_name="comick_comiklist")