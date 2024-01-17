/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package it.unipi.dsmt;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class Main {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment

		try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {

			// kafka consumer
			Properties consumerProperties = new Properties();
			consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
					"kafka:9093");
			consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());

			FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
					"orders",             // source Kafka topic
					new SimpleStringSchema(),   // deserialization schema
					consumerProperties
			);

			DataStream<String> kafkaStream = env.addSource(kafkaConsumer);
			kafkaStream.print();

			env.execute("Flink Streaming Java API Skeleton");

		}
	}
}

