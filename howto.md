### How to deploy and run the application

inside the `docker` directory, run:
`docker compose up -d`

#### Kafka
To view messages on a kafka topic:
- Run the `orders-simulator` project
- enter inside kafka container `docker exec -it <kafka_conatiner_id> sh`
- `cd opt/kafka/bin`
- to view messages on a certain topc `kafka-console-consumer.sh --topic topic_name --from-beginning --bootstrap-server localhost:9092`
  