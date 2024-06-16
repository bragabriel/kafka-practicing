package org.practicing.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ConsumerParallel {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var consumer = new KafkaConsumer<String, String>(properties());

        consumer.subscribe(Collections.singletonList("ecommerce.compras"));

        while(true){
            var records = consumer.poll(Duration.ofMillis(100));
            for(var record : records){
                System.out.println("Compra nova: ");
                System.out.println(record.key());
                System.out.println(record.value());
                System.out.println(record.offset());
                System.out.println(record.partition());
            }
        }

    }
    private static Properties properties(){
        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //pega desde o começo as mensagens
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "ecommerce-teste"); //Informando o grupo que vamos consumir as msgs

        return properties;
    }
}
