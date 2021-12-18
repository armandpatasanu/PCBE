package servermess;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ChatServer {

    private final static Consumer<String, String> consumer = KafkaConfig.getServerConsumer();
    private static final Producer<String, String> kafkaProducer = KafkaConfig.getProducer();
    private static final Logger LOGGER = LoggerFactory.getLogger(Messagereceiver.class);
    private static AbstractMap<String, Long> users = new ConcurrentHashMap<>();
    private static ArrayList<String> topicList = new ArrayList<String>();


    private static final int GIVE_UP = 100;

    public static void main(String[] args) {
        Thread user = new Thread(){
            public void run()
            {
                TopicUtils topicCreator = new TopicUtils();
                if (!topicCreator.checkTopicExist(KafkaConstants.NICKNAMES_TOPIC))
                    topicCreator.createTopic(KafkaConstants.NICKNAMES_TOPIC);
                if (!topicCreator.checkTopicExist(KafkaConstants.PING_TOPIC))
                    topicCreator.createTopic(KafkaConstants.PING_TOPIC);
                consumer.subscribe(Collections.singleton(KafkaConstants.NICKNAMES_TOPIC));
                PingHandler p = new PingHandler(users); //in const sa fie conc. hahsmap
                p.start();
                handleMessages();
            }
        };
        user.start();
    }
//verif ca nu exista deja topicul

    public static void handleMessages() {
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(200));

            for (ConsumerRecord<String, String> record : consumerRecords) {
                String command = record.value();
                if (command.startsWith("NICK/")) {
                    handleUserCreation(command);
                }
                else if (command.startsWith("SUBSCRIBE/")){
                    handleTopicSubscription(command);

                }
                else
                {
                    System.out.println(record.topic() + " " + record.value());
                }
            }

            /*consumerRecords.forEach(consumerRecord -> LOGGER.info("Consumer Record:({} {})",
                    consumerRecord.key(), consumerRecord.value(),
                    consumerRecord.partition(), consumerRecord.offset()));*/

            //consumer.commitSync();


        //consumer.close();

        //LOGGER.info("Done");
        }
    }

    public static boolean topicAlreadyExists(String topic)
    {
        for (String t : topicList)
        {
            if (t.equals(topic))
            {
                return true;
            }
        }
        return false;
    }

    public static void handleUserCreation(String command)
    {
        String commandRemoved = command.substring(5);
        String parts[] = commandRemoved.split("\\*");
        String nickname = parts[0];
        String userId = parts[1];
//      User user = new User(nickname, UUID.fromString(userId));
        //System.out.println(user.getNickname());
        System.out.println("User:" + userId);
        String topic = KafkaConstants.SERVER_CLIENT_TOPIC + "-" + nickname;
        System.out.println("Topic" + topic);
        ProducerRecord<String, String> response = new ProducerRecord<>(topic,"User was created successfully!");
        kafkaProducer.send(response);
        kafkaProducer.flush();
    }

    public static void handleTopicSubscription(String command)
    {
        TopicUtils topicUtils = new TopicUtils();
        String commandRemoved = command.substring(10);
        System.out.println(commandRemoved);
        String parts[] = commandRemoved.split("\\*");
        String topicToSubscribe = parts[0];
        if (!topicUtils.checkTopicExist(topicToSubscribe))
        {
            topicUtils.createTopic(topicToSubscribe);
        }
        if (!topicAlreadyExists(topicToSubscribe))
        {
            topicList.add(topicToSubscribe);
        }
        String nickname = parts[1];
        String topic = KafkaConstants.SERVER_CLIENT_TOPIC + "-" + nickname;
        ProducerRecord<String, String> response = new ProducerRecord<>(topic,"SUBSCRIBE/"+topicToSubscribe);
        kafkaProducer.send(response);
        kafkaProducer.flush();
    }
}
