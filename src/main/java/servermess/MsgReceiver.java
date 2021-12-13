package servermess;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;

public class MsgReceiver extends Thread {

    private final Consumer<String, String> consumer;
    private static String serverTopic;

    public MsgReceiver(UUID userId, String topic) {

        consumer = KafkaConfig.getConsumer(userId);
        serverTopic = topic;
        consumer.subscribe(Collections.singleton(topic));
        System.out.println("Initialised mess receiver" + topic);
    }

    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                //System.out.println("Polling");
                for (ConsumerRecord<String, String> record : records) {
                    if (record.topic().equals(serverTopic))
                    {
                        if (record.value().startsWith("SUBSCRIBE/"))
                        {
                            String topicToSubscribe = record.value().substring(10);
                            String subscribedTopic = KafkaConstants.TOPICS_TOPIC + "-" + topicToSubscribe;
                            consumer.subscribe(Collections.singleton(subscribedTopic));
                            System.out.println("Subscribed to "+ subscribedTopic);
                        }
                    }
                    System.out.printf("offset = %d, key = %s, value = %s%n topic = %s%n", record.offset(), record.key(), record.value(), record.topic());
                }
            }
        } finally {
            consumer.close();
        }
    }

}


