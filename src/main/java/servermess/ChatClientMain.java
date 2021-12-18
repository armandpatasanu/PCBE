package servermess;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.*;

import static org.apache.kafka.common.utils.Utils.sleep;

public class ChatClientMain {

    private static boolean isLoggedIn = false;
    private static String nickname = "";
    private static final UUID userId = UUID.randomUUID();
    private static final Producer<String, String> kafkaProducer = KafkaConfig.getProducer();
    private static Map<String, MsgReceiver> topicThreadMap = new HashMap<String, MsgReceiver>();
    private static ArrayList<String> topicList = new ArrayList<String>();
    private static ChatClient client;
    private static Consumer<String, String> consumer = KafkaConfig.getConsumer(userId);
    private static TopicUtils topicCreator = new TopicUtils();
    public static void main(String[] args)
    {

        String topic = "";
        do {

            //TO DO de verificat daca exista nicknameul
            System.out.println("Please pick a nickname:");
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(System.in));
            try {
                nickname = reader.readLine();
                if (nickname.length() != 0) {
                    isLoggedIn = true;
                    topic = KafkaConstants.SERVER_CLIENT_TOPIC + "-" + nickname;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        } while (!isLoggedIn);
        if (!topicCreator.checkTopicExist(KafkaConstants.FETCHTOPICS_TOPIC + "-" + nickname))
            topicCreator.createTopic(KafkaConstants.FETCHTOPICS_TOPIC + "-" + nickname);
        consumer.subscribe(Collections.singleton(KafkaConstants.FETCHTOPICS_TOPIC + "-" + nickname));
        if (!topicCreator.checkTopicExist(KafkaConstants.FETCHUSERS_TOPIC + "-" + nickname))
            topicCreator.createTopic(KafkaConstants.FETCHUSERS_TOPIC + "-" + nickname);
        consumer.subscribe(Collections.singleton(KafkaConstants.FETCHUSERS_TOPIC + "-" + nickname));
        client = new ChatClient(userId, topic, nickname);
        Thread t = new Thread(client);
        t.start();
        startChat();
    }

    public static void startChat() {
        printMenu();
        boolean stop = false;
        do {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(System.in));
            try {
                String option = reader.readLine();
                switch (option) {
                    case "1":
                        listTopics();
                        break;
                    case "2":
                        listUsers();
                        break;
                    case "3":
                        pickTopic();
                        break;
                    case "4":
                        sendMessage();
                        break;
                    case "5":
                        System.out.println("Exiting..");
                        stop = true;
                    default:
                        System.out.println("Unrecognized command");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        } while (!stop);
    }

    public static void printMenu() {
        System.out.println("Select the option number:");
        System.out.println("1. List topics");
        System.out.println("2. List online users");
        System.out.println("3. Join topic");
        System.out.println("4. Message user or topic");
        System.out.println("5.Exit");
    }

    public static void listTopics() {
        String searched_msg ="";
        boolean condition = false;
        UUID message_id = UUID.randomUUID();
        ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConstants.NICKNAMES_TOPIC, "FETCHTOPICS/" + nickname + "*" + message_id.toString());
        kafkaProducer.send(record);

        sleep(1500);
        //System.out.println(message_id);
        do {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            //System.out.println("Polling");
            for (ConsumerRecord<String, String> r: records)
            {
                if(r.value().contains(message_id.toString()))
                {
                    //System.out.println("Found it!");
                    condition = true;
                    searched_msg = r.value();
                }
            }
        }while(!condition);

        String parts[] = searched_msg.split("\\*");
        int numberOfTopics = Integer.parseInt(parts[0]);
        System.out.println("Available topics are:");
        for(int i = 1;i<numberOfTopics-1;i++) {
            String myTopic = parts[i].replace(KafkaConstants.TOPICS_TOPIC, "");
            System.out.println(myTopic);
        }
    }

    public static void sendMessage() {
        System.out.println("Do you want to message a user or a topic?");
        System.out.println("1. User");
        System.out.println("2. Topic");
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(System.in));
        try {
            String option = reader.readLine();
            switch (option)
            {
                case "1":
                    messageTopic();
                    break;
                case "2":
                    messageUser();
                    break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void messageTopic()
    {
        System.out.println("Write topic:");
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(System.in));
        try {
            String topic = reader.readLine();
            System.out.println("Write your message:");
            String message = reader.readLine();
            ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConstants.TOPICS_TOPIC+"-"+topic, message);
            kafkaProducer.send(record);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void messageUser()
    {
    }

    public static void pickTopic() {
        listTopics();
        System.out.println("Please write an existing topic name or a new one");
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(System.in));
        try {
            String topicToAdd = reader.readLine();
            ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConstants.NICKNAMES_TOPIC, "SUBSCRIBE/"+topicToAdd+"*"+nickname);
            kafkaProducer.send(record);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public static void listUsers() {
        String searched_msg ="";
        boolean condition = false;
        UUID message_id = UUID.randomUUID();
        ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConstants.NICKNAMES_TOPIC, "FETCHUSERS/" + nickname + "*" + message_id.toString());
        kafkaProducer.send(record);

        sleep(1500);
        //System.out.println(message_id);
        do {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            //System.out.println("Polling");
            for (ConsumerRecord<String, String> r: records)
            {
                if(r.value().contains(message_id.toString()))
                {
                    //System.out.println("Found it!");
                    condition = true;
                    searched_msg = r.value();
                }
            }
        }while(!condition);

        String parts[] = searched_msg.split("\\*");
        int numberOfTopics = Integer.parseInt(parts[0]);
        System.out.println("Available users are:");
        for(int i = 1;i<numberOfTopics-1;i++) {
            String myTopic = parts[i].replace(KafkaConstants.SERVER_CLIENT_TOPIC + "-", "");
            System.out.println(myTopic);
        }
    }
}
