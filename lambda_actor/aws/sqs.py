from typing import List
import boto3

def send(queue, msg_list):
    size = 10
    for start in range(0, len(msg_list), size):
        queue.send_messages(Entries=msg_list[start: start + size])

def send_execute_message(queue, message_body: str, executor_id: int, executor_key: str):
    message_group_id = f"group_{executor_key}_{str(executor_id)}"
    msg_list = [{'Id': "0", 'MessageBody': message_body, 'MessageGroupId': message_group_id}]
    send(queue=queue, msg_list=msg_list)

def send_executor_task_message(queue, message_body_list: List[str], executor_key: str):
    msg_num = len(message_body_list)
    message_group_id = f"group_{executor_key}"
    msg_list = [{'Id': '{}'.format(i+1), 'MessageBody': message_body_list[i], 'MessageGroupId': message_group_id} for i in range(msg_num)]
    send(queue=queue, msg_list=msg_list)

def send_driver_trigger_message(queue, message_body: str, executor_id: int, executor_key: str):
    message_group_id = f"group_{executor_key}_{str(executor_id)}"
    msg_list = [{'Id': "0", 'MessageBody': message_body, 'MessageGroupId': message_group_id}]
    send(queue=queue, msg_list=msg_list)

def receive(queue, fetch_count=1) -> List[str]:
    msg_list = queue.receive_messages(MaxNumberOfMessages=fetch_count)
    def __get_q(m):
        s = m.body
        m.delete()
        return s
    message_body = list(map(lambda m: __get_q(m), msg_list))
    return message_body

def get(queue, fetch_count=1) -> List[str]:
    msg_list = queue.receive_messages(MaxNumberOfMessages=fetch_count)
    def __get_q(m):
        s = m.body
        return s
    message_body = list(map(lambda m: __get_q(m), msg_list))
    return message_body

def receive_all(queue) -> List[str]:
    message_body = []
    while True:
        # メッセージを取得
        msg_list = queue.receive_messages(MaxNumberOfMessages=10)
        if msg_list:
            for message in msg_list:
                message_body.append(message.body)
                message.delete()
        else:
            break
    return message_body

def get_q_current_size(qname: str):
    # ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible ApproximateNumberOfMessagesDelayed
    m = boto3.client('sqs', region_name='ap-northeast-1').get_queue_attributes(QueueUrl=qname, AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed"])
    message_count = m['Attributes']['ApproximateNumberOfMessages']
    return message_count

def is_q_empty(qname: str) -> bool:
    q_size = int(get_q_current_size(qname=qname))
    if q_size > 0:
        return True
    else:
        return False


