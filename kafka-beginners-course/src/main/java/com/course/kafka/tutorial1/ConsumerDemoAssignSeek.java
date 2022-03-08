package com.course.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;

public class ConsumerDemoAssignSeek {

    private final static String kafkaServer = "localhost:9092";
    private final static String groupId = "group_1";
    private final static String topic = "first_topic";
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);


    public static void main(String[] args) {

        logger.info("Iniciando");

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST.toString().toLowerCase(Locale.ROOT));

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 25L;
        consumer.assign(Arrays.asList(partitionToReadFrom));
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessageReadSoFar = 0;

        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                numberOfMessageReadSoFar += 1;
                logger.info("key: " + record.key() + " | Value: " + record.value());
                logger.info("Partition: " + record.partition() + "| Offset: " + record.offset());
                if (numberOfMessageReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }

    }
}
