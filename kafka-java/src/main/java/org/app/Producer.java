package org.app;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {
    static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);
    static final String localhost="localhost:9092,localhost:9093,localhost:9094";
    public static void main(String[] args) throws Exception{
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,localhost );
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
//        int i=501;
        for (int i = 0; i < 1000; ++i) {
//        while(true){

            ProducerRecord<String, String> record = new ProducerRecord<>("apr10", "key", "NEWsssss"+i);

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        LOGGER.info("\nRecivied Record Meta Data. \n" +
                                "Topic: " + recordMetadata.topic() + " , Partition " + recordMetadata.partition() + ", " +
                                "Offset: " + recordMetadata.offset() + ", TimeStamp: " + recordMetadata.timestamp() + "\n");
                    } else {
                        LOGGER.error("ERROR OCCURED", e);

                    }
                }
            });
            System.out.println("New-Message "+i+" Pushed");
            Thread.sleep(1000);
//            i++;
        }
//        producer.flush();
//        producer.close();
    }
}
