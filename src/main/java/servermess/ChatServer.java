package servermess;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class ChatServer {

    private final static Consumer<String, String> consumer = KafkaConfig.getServerConsumer();
    private static final Producer<String, String> kafkaProducer = KafkaConfig.getProducer();
    private static AbstractMap<String, Long> users = new ConcurrentHashMap<>();
    private static ArrayList<String> topicList = new ArrayList<String>();

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
                else if (command.startsWith("FETCHTOPICS/")) {
                    handleListTopicRequest(command);
                } else if (command.startsWith("FETCHUSERS/")){
                    handleListUsersRequest(command);
                }
                else if (command.startsWith("PM/"))
                {
                    handleMessageUserRequest(command);

                }
                else if (command.startsWith("CHECK/"))
                {
                    checkNicknameExists(command);
                }
                else if (command.startsWith("TM/"))
                {
                    handleMessageTopicRequest(command);
                }
                else
                {
                    System.out.println(record.topic() + " " + record.value());
                }
            }
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

    public static void handleListTopicRequest(String command)
    {
        int topics_count = 0;
        Set<String> topics = new HashSet<>();
        TopicUtils topicUtils = new TopicUtils();
        String commandRemoved = command.substring(12);
        String parts[] = commandRemoved.split("\\*");
        String nickname = parts[0];
        try {
            topics = topicUtils.getTopics();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String topic = KafkaConstants.SERVER_CLIENT_TOPIC + "-" + nickname;
        Iterator<String> it = topics.iterator();

        while(it.hasNext()) {
            String single_topic = it.next();
            if (single_topic.startsWith(KafkaConstants.TOPICS_TOPIC))
            {
                topics_count++;
            }
        }
        String final_message = "TOPICS/" + topics_count + "*";

        Iterator<String> it1 = topics.iterator();
        while(it1.hasNext()) {
            String s_topic = it1.next();
            if (s_topic.startsWith(KafkaConstants.TOPICS_TOPIC))
            {
                final_message = final_message + s_topic + "*";
            }
        }
        System.out.println(final_message);
        ProducerRecord<String, String> response = new ProducerRecord<>(topic, final_message);
        kafkaProducer.send(response);

    }

    public static void handleListUsersRequest(String command)
    {
        String nickname = command.substring(11);
        String topic = KafkaConstants.SERVER_CLIENT_TOPIC + "-" + nickname;
        int topics_count = 0;
        String userList = "";
        for (String user : users.keySet())
        {
            userList = userList + user + "*";
            topics_count++;
        }
        String final_message = "USERS/" + topics_count + "*" + userList;
        ProducerRecord<String, String> response = new ProducerRecord<>(topic, final_message);
        kafkaProducer.send(response);


    }

    public static void handleMessageUserRequest(String command)
    {
        String commandRemoved = command.substring(3);
        String parts[] = commandRemoved.split("\\*");
        String userToMsg = parts[0];
        String msgToSend = parts[1];
        String sender = parts[2];
        String topic = KafkaConstants.SERVER_CLIENT_TOPIC + "-" + sender;
        if (!users.containsKey(userToMsg))
        {
            ProducerRecord<String, String> response = new ProducerRecord<>(topic, "Sorry, " + userToMsg + " is not online!");
            kafkaProducer.send(response);
        }
        else
        {
            String msgTopic = KafkaConstants.USER_TOPIC+"-"+userToMsg;
            ProducerRecord<String, String> message = new ProducerRecord<>(msgTopic, "@"+sender+": " + msgToSend);
            kafkaProducer.send(message);
        }
    }

    public static void handleMessageTopicRequest(String command){
        String commandRemoved = command.substring(3);
        String parts[] = commandRemoved.split("\\*");
        String topicToMsg = parts[0];
        String msg = parts[1];
        String sender = parts[2];
        String topic = KafkaConstants.SERVER_CLIENT_TOPIC + "-" + sender;
        if (!topicAlreadyExists(topicToMsg))
        {
            ProducerRecord<String, String> response = new ProducerRecord<>(topic, "Sorry, the topic " + topicToMsg + " doesn't exist. Please create it first!");
            kafkaProducer.send(response);
        }
        else
        {
            String msgTopic = KafkaConstants.TOPICS_TOPIC+"-"+topicToMsg;
            ProducerRecord<String, String> message = new ProducerRecord<>(msgTopic, "Topic: " + topicToMsg +"\n@"+sender+": " + msg);
            kafkaProducer.send(message);
        }


    }

    public static void handleUserCreation(String command)
    {
        String commandRemoved = command.substring(5);
        String parts[] = commandRemoved.split("\\*");
        String nickname = parts[0];
        String userId = parts[1];
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

    public static void checkNicknameExists(String command)
    {
        String commandRemoved = command.substring(6);
        String parts[] = commandRemoved.split("\\*");
        String nicknameToCheck = parts[0];
        String userId = parts[1];
        String msgId = parts[2];
        if (users.containsKey(nicknameToCheck))
        {
            ProducerRecord<String, String> response = new ProducerRecord<>(KafkaConstants.CHECK_NICK+"-"+userId, "0*" + msgId);
            kafkaProducer.send(response);
        }
        else
        {
            ProducerRecord<String, String> response = new ProducerRecord<>(KafkaConstants.CHECK_NICK+"-"+userId, "1*" + msgId);
            kafkaProducer.send(response);
        }
    }
}
