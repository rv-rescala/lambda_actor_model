from dataclasses import dataclass
import json


@dataclass
class ActorConf:
    """[summary]
    """
    driver_fifo: str
    driver_status_fifo: str
    executor_fifo: str
    executor_concurrency: int
    max_retry: int
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
                driver_fifo=actor_conf_json["driver_fifo"],
                executor_fifo=actor_conf_json["executor_fifo"],
                driver_status_fifo=actor_conf_json["driver_status_fifo"],
                executor_concurrency=actor_conf_json["executor_concurrency"],
                max_retry=actor_conf_json["max_retry"],
                trigger_file_bucket=actor_conf_json["trigger_file_bucket"],
                trigger_file_prefix=actor_conf_json["trigger_file_prefix"],
                trigger_file = actor_conf_json["trigger_file"],
                )
        return actor_conf

