# coding=utf-8
import json
import random
import time
import codecs
from kafka import KafkaProducer


log_file = "/opt/access.log"
topic = 'topic'

kafka_server = 'ip:9092'

user_count = 100
log_count = 100
ip = [127, 156, 222, 105, 24, 192, 153, 127, 31, 168, 32, 10, 82, 77, 118, 228]
status_code = ("200",)
url_count = 10
content_uri_pattern = '/nanHu/contents/{content_id}?user_id={user_id}'


# random time
def sample_time():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


# random user
def sample_users():
    # 假设有1000W注册用户，每日访问用户10W-50W人
    all_users = range(1, user_count)
    user_cont = random.randint(10, 50)
    users = random.sample(all_users, user_cont)
    return users


# random ip
def sample_ip():
    random_ip = random.sample(ip, 4)
    return ".".join([str(item) for item in random_ip])


# random code
def sample_status_code():
    return random.sample(status_code, 1)[0]


# random station_time
def sample_station_time():
    return random.randint(15, 60)


# random score
def sample_score():
    return random.randint(30, 100)


def generate_log(count=10):
    time_str = sample_time()
    users = sample_users()
    print('Start generate [%s] log..' % log_count)
    producer = KafkaProducer(bootstrap_servers=kafka_server)

    with codecs.open(log_file, "a+", encoding='utf-8') as f:

        while count >= 1:

            user_id = random.choice(users)
            sample_content_id = str(random.randint(0, url_count))

            ret_url = content_uri_pattern.format(
                content_id=sample_content_id,
                user_id=user_id
            )
            query_log = u"{ip} [{local_time}] \"GET {url} HTTP/1.1\" {status_code}".format(
                url=ret_url,
                ip=sample_ip(),
                status_code=sample_status_code(),
                local_time=time_str
            )
            f.write(query_log + u'\n')

            event_log = {
                "station_time": str(sample_station_time()),
                "user_id": user_id,
                "score": sample_score(),
                "local_time": time_str
            }

            producer.send(topic, json.dumps(event_log).encode('utf-8'))
            if count % 100 == 0:
                print('generate msgs: [%s]' % count)
            count = count - 1

    producer.close()
    print('Finish generate log [%s]' % log_count)


if __name__ == '__main__':
    try:
        generate_log(log_count)
    except Exception as e:
        print(str(e))
        exit(-1)
