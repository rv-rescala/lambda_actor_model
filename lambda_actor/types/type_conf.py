from dataclasses import dataclass
import json


@dataclass
class ActorConf:
    """[summary]
    """
    driver_trigger_q: str
    executor_trigger_q: str
    executor_task_q: str
    executor_result_q: str
    executor_concurrency: int
    max_retry: int
    max_lambda_execution_time: int
    trigger_file_bucket: str
    trigger_file_prefix: str
    trigger_file: str

    @classmethod
    def get_actor_conf(cls, s3_client, bucket: str, prefix: str, filename: str):
        """[summary]

        Args:
            s3_client ([type]): [description]
            bucket (str): [description]
            prefix (str): [description]
            filename (str): [description]

        Returns:
            ActorConf: [description]
        """
        tmp_path = f'/tmp/{filename}'
        s3_client.download_file(bucket, f'{prefix}/{filename}', tmp_path)
        with open(tmp_path, encoding='utf-8') as f:
            actor_conf_json = json.load(f)
            actor_conf = ActorConf(
                driver_trigger_q=actor_conf_json["driver_trigger_q"],
                executor_trigger_q=actor_conf_json["executor_trigger_q"],
                executor_task_q=actor_conf_json["executor_task_q"],
                executor_result_q=actor_conf_json["executor_result_q"],
                executor_concurrency=actor_conf_json["executor_concurrency"],
                max_retry=actor_conf_json["max_retry"],
                max_lambda_execution_time=actor_conf_json["max_lambda_execution_time"],
                trigger_file_bucket=actor_conf_json["trigger_file_bucket"],
                trigger_file_prefix=actor_conf_json["trigger_file_prefix"],
                trigger_file = actor_conf_json["trigger_file"],
                )
        return actor_conf

