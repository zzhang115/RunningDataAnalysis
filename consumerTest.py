from kafka import KafkaConsumer


def main():
    consumer = KafkaConsumer(b"stock-analyzer", group_id=b"my_group_id",
                             metadata_broker_list=["192.168.99.100:9092"])
    for message in consumer:
        # This will wait and print messages as they become available
        print(message)

if __name__ == "__main__":
    main()