from kafka import KafkaConsumer

consumer = KafkaConsumer("user_log_topic", group_id="group", bootstrap_servers=["192.168.5.193:9092"])
total_messages_count = 0
running = True

while running:
    msg_pack = consumer.poll(timeout_ms=500)
    for tp, messages in msg_pack.items():
        for message in messages:
            # print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value.decode('utf-8')), decode(message.value))
            print(message.value.decode('utf-8'))
            total_messages_count += 1
            print(f"{'-'*10} {total_messages_count} {'-'*10} ")

# validate kafka

# kafka to spark

# """
#
# """