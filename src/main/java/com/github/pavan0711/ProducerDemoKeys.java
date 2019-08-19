package com.github.pavan0711;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        String bootstrapServer = "0.0.0.0:9092";

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        for(int i=0; i<10; i++) {

            String topic = "my-first-topic";
            String value = "Hello World Keys " + i ;
            String key = "id_" + i;

            //Create Producer Data
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key,
                    value);

            //Same Key Always Goes To Same Partition
            LOGGER.info("Key = {}", key);

            //Send Data
            kafkaProducer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        LOGGER.error("Error Occurred While Trying To Push Message {}", e.getLocalizedMessage(), e);
                    } else {
                        LOGGER.info("Message Pushed To Kafka Successfully! \n Partition = {} " +
                                        "\n Offset = {} \n Topic = {}", recordMetadata.partition(), recordMetadata.offset(),
                                recordMetadata.topic());
                    }
                }
            }).get(); //Block Send To Make Sync, Not To Be Used In Prod
        }
        //Flush And Close Producer
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
