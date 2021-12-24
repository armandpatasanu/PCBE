package servermess;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.Consumer;

import java.time.Duration;
import java.util.*;

public class MsgReceiver extends Thread {

    private final Consumer<String, String> consumer;
    private String serverTopic;
    private ArrayList<String> subscribedTopics = new ArrayList<>();

    public MsgReceiver(UUID userId, String topic, String nickname) {

        consumer = KafkaConfig.getConsumer(userId);
        serverTopic = topic;
        subscribedTopics.add(topic);
        subscribedTopics.add(KafkaConstants.USER_TOPIC + "-" + nickname);
        consumer.subscribe(subscribedTopics);
        System.out.println("Initialised mess receiver" + topic);
    }

    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    if (record.value().startsWith("SUBSCRIBE/")) {
                        String topicToSubscribe = record.value().substring(10);
                        String subscribedTopic = KafkaConstants.TOPICS_TOPIC + "-" + topicToSubscribe;
                        subscribedTopics.add(subscribedTopic);
                        consumer.subscribe(subscribedTopics);
                        System.out.println("Subscribed to " + subscribedTopic);
                    } else if (record.value().startsWith("TOPICS/")) {
                        String searched_msg = record.value().substring(7);
                        String parts[] = searched_msg.split("\\*");
                        int numberOfTopics = Integer.parseInt(parts[0]);
                        System.out.println("Available topics are:");
                        for (int i = 1; i < numberOfTopics + 1; i++) {
                            String myTopic = parts[i].replace(KafkaConstants.TOPICS_TOPIC, "");
                            System.out.println(myTopic);
                        }
                    }
                    else if (record.value().startsWith("USERS/")) {
                        String searched_msg = record.value().substring(6);
                        String parts[] = searched_msg.split("\\*");
                        int numberOfTopics = Integer.parseInt(parts[0]);
                        System.out.println("Available users are:");
                        for (int i = 1; i < numberOfTopics + 1; i++) {
                            String myTopic = parts[i];
                            System.out.println(myTopic);
                        }
                    }
                    else
                    {
                        System.out.println(record.value());
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

}


