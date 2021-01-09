#!/usr/bin/env python
# -*- coding: utf-8 -*-
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import datetime
'''
 @author rongqing.yang
 @date 2021/01/07
 @comment 生产或消费kafka测试数据
'''
class Kafka_producer():
    '''
    使用kafka的生产模块
    '''

    def __init__(self, kafkaaddress,kafkatopic,kafkadir):
        self.kafkaaddress = kafkaaddress
        self.kafkatopic = kafkatopic
        self.kafkadir = kafkadir

    def sendjsondata(self):
        try:
            print(self.kafkaaddress)
            producer = KafkaProducer(
                bootstrap_servers = self.kafkaaddress.split(','),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'))
            file = open(self.kafkadir, 'rb')
            context = file.readline()
            print(context)
            data = json.loads(context)
            producer.send(self.kafkatopic, data, partition=0)
            producer.close()
        except KafkaError as e:
            print(e)


class Kafka_consumer():
    '''
    使用Kafka—python的消费模块
    '''

    def __init__(self, kafkaaddress, kafkatopic, groupid, filedir):
        self.kafkaaddress= kafkaaddress
        self.kafkatopic = kafkatopic
        self.groupid = groupid
        self.filedir= filedir
        self.consumer = KafkaConsumer(self.kafkatopic, auto_offset_reset='latest',group_id = self.groupid,
                                      bootstrap_servers = self.kafkaaddress.split(','))
        today = datetime.date.today()
        suffix = today.strftime('%y-%m-%d')
        self.filename = '%s/%s-%s.log' % (self.filedir, self.kafkatopic, suffix)

    def consume_data(self):
        try:
            for message in self.consumer:
                #print(self.filename)
                value = message.value.decode().replace('\n', '')
                #解决非法字符问题
                value = value.encode('GBK', 'ignore').decode('GBk')
                file = open(self.filename, 'a')
                print(value, file=file)
                #print(value)
                file.close()

        except KeyboardInterrupt as e:
            print(e)


def main():
    '''
    测试consumer和producer
    :return:
    '''
    ##测试生产模块
    #测试生产者需输入集群地址、topic、消息文件绝对路径3个参数（topic测试一般用test001）
    producer = Kafka_producer("172.16.152.181:9092,172.16.152.178:9092,172.16.152.182:9092", "test","D:/test1.txt")
    producer.sendjsondata()
    ##测试消费模块
    # 测试消费者者需输入依次为集群地址、topic、groupid、消息文件输出到本地目录4个参数，（topic测试一般用test001，groupid需自定义）
    # consumer = Kafka_consumer("172.16.152.181:9092,172.16.152.178:9092,172.16.152.182:9092", "test", 'yrqtest','D:/temp')
    # consumer.consume_data()



if __name__ == '__main__':
    main()
