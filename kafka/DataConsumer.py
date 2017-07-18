from kafka import KafkaConsumer

topic = 'stock-analyzer'
new_topic = 'average-stock-analyzer'
consumer=KafkaConsumer(new_topic, bootstrap_servers='192.168.99.100:9092')
for msg in consumer:
    print(msg)