from kafka import KafkaConsumer

consumer = KafkaConsumer("user_log_after", group_id="group", bootstrap_servers=["localhost:9092"])

running = True

while running:
    msg_pack = consumer.poll(timeout_ms=500)
    for tp, messages in msg_pack.items():
        for message in messages:
            # print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value.decode('utf-8')), decode(message.value))
            print(message.value.decode('utf-8'))