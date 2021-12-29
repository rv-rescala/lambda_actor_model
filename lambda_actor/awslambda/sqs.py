from typing import List


def enqueue(queue, message_body: List[str]):
    msg_num = len(message_body)
    msg_list = [{'Id': '{}'.format(i+1), 'MessageBody': message_body[i], 'MessageGroupId': 'group'} for i in range(msg_num)]
    size = 10
    for start in range(0, len(msg_list), size):
        queue.send_messages(Entries=msg_list[start: start + size])


def dequeue(queue, fetch_count=1) -> List[str]:
    msg_list = queue.receive_messages(MaxNumberOfMessages=fetch_count)
    def __get_q(m):
        s = m.body
        m.delete()
        return s
    message_body = list(map(lambda m: __get_q(m), msg_list))
    return message_body


def dequeue_all(queue) -> List[str]:
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
