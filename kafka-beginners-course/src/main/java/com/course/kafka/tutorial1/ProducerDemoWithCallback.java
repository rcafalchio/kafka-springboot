package com.course.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private final static String kafkaServer = "localhost:9092";
    private static Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {

            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello world " + i);

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {

                    if (exception == null) {
                        logger.info("Received new metadata. \n" + "TOPIC: " + metadata.topic() + "\n"
                                + "Partition: " + metadata.partition() + "\n"
                                + "Offset: " + metadata.offset() + "\n"
                                + "Timestamp: " + metadata.timestamp());
                    } else {
                        logger.error(exception.getMessage(), exception);
                    }

                }
            });
            producer.flush();
        }

        producer.close();
    }
}
