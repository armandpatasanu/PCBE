package servermess;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.*;

import static org.apache.kafka.common.utils.Utils.sleep;

public class ChatClientMain {

    private static boolean isLoggedIn = false;
    private static String nickname = "";
    private static final UUID userId = UUID.randomUUID();
    private static final Producer<String, String> kafkaProducer = KafkaConfig.getProducer();
    private static ChatClient client;
    private static Consumer<String, String> consumer = KafkaConfig.getConsumer(UUID.randomUUID());
    private static TopicUtils topicCreator = new TopicUtils();
    public static void main(String[] args)
    {
        if (!topicCreator.checkTopicExist(KafkaConstants.CHECK_NICK + "-" + userId))
            topicCreator.createTopic(KafkaConstants.CHECK_NICK + "-" + userId);
        consumer.subscribe(Collections.singleton(KafkaConstants.CHECK_NICK + "-" + userId));
        String topic = "";
        do {

            System.out.println("Please pick a nickname:");
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(System.in));
            try {
                nickname = reader.readLine();
                if (nickname.length() != 0 && checkNickname(nickname)) {
                    isLoggedIn = true;
                    topic = KafkaConstants.SERVER_CLIENT_TOPIC + "-" + nickname;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        } while (!isLoggedIn);
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
                        printMenu();
                        break;
                    case "6":
                        System.out.println("Exiting..");
                        stop = true;
                        break;
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
        System.out.println("5. Print menu");
        System.out.println("6. Exit");
    }

    public static void listTopics() {
        ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConstants.NICKNAMES_TOPIC, "FETCHTOPICS/" + nickname);
        kafkaProducer.send(record);

    }

    public static void sendMessage() {
        System.out.println("Do you want to message a user or a topic?");
        System.out.println("1. Topic");
        System.out.println("2. User");
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
                default:
                    System.out.println("not an option");
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
            ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConstants.NICKNAMES_TOPIC, "TM/" + topic + "*" + message + "*" + nickname);
            kafkaProducer.send(record);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void messageUser()
    {
        System.out.println("Select user:");
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(System.in));
        try {
            String user = reader.readLine();
            System.out.println("Write your message:");
            String msg = reader.readLine();
            ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConstants.NICKNAMES_TOPIC, "PM/"+user+"*"+msg+"*"+nickname);
            kafkaProducer.send(record);
        } catch (IOException e) {
            e.printStackTrace();
        }
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
        ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConstants.NICKNAMES_TOPIC, "FETCHUSERS/" + nickname);
        kafkaProducer.send(record);
    }

    public static boolean checkNickname(String nickname)
    {
        String searched_msg ="";
        boolean condition = false;
        UUID message_id = UUID.randomUUID();
        ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConstants.NICKNAMES_TOPIC, "CHECK/" + nickname + "*" + userId +"*" + message_id.toString());
        kafkaProducer.send(record);
        sleep(1500);
        //System.out.println(message_id);
        do {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> r: records)
            {
                if(r.value().contains(message_id.toString()))
                {
                    condition = true;
                    searched_msg = r.value();
                }
            }
        }while(!condition);

        return !searched_msg.startsWith("0");
    }
}
