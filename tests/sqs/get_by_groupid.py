from typing import List
import boto3

def receive(queue, fetch_count=1) -> List[str]:
    def __get_q(m):
        s = m.body
        m.delete()
        return s

    msg_list = queue.receive_messages(MaxNumberOfMessages=fetch_count)
    if len(msg_list) == 0:
        return None
    else:
        message_body = list(map(lambda m: __get_q(m), msg_list))
        return message_body

sqs = boto3.resource('sqs', region_name='ap-northeast-1')
queue = sqs.get_queue_by_name(QueueName="executor_task_q.fifo")
while True:
    m = receive(queue)
    print(m)
    if not m:
        break


