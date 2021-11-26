package com.servermesagerie;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

public class ChatClient {

    public static void main(String[] args)
    {
        final Logger logger = LoggerFactory.getLogger(Producer.class);

        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        for (int i = 0; i< 10; ++i)
        {
            ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "key_" + i, "value_" + i);

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null)
                    {
                        logger.info("\nReceived record metadata.\n" +
                                "Topic: " + recordMetadata.topic() + ", Partition: " + recordMetadata.partition() + ", " +
                                "Offset: " + recordMetadata.offset() + " @ Timestamp: " + recordMetadata.timestamp() + "\n");
                    }
                    else
                    {
                        logger.error("Error occured ", e);
                    }
                }
            });
        }

        producer.flush();
        producer.close();
    }

    /*
    private static User user;
    private static KafkaProducer<String, String> producer;

    public static void main(String[] args)
    {
        System.out.println("Starting client....");
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serializa-tion.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serializa-tion.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
        try
        {
            startChat();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally {
            producer.close();
        }

    }

    public static void startChat() throws Exception
    {
        System.out.println("-Client started-");
        String topic="test";
        String message="AICI TESTAM";
        ProducerRecord<String,String> record =
                new ProducerRecord<String,String>(topic,
                        message);
        producer.send(record);
    }

    public static void login(String nickname)
    {
        user = new User(nickname);
    }

    public static void subscribeToTopic(String topic)
    {

    }
    */
}
