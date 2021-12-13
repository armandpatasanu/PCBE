package servermess;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ChatClientMain {

    private static boolean isLoggedIn = false;
    private static String nickname = "";
    private static final UUID userId = UUID.randomUUID();
    private static final Producer<String, String> kafkaProducer = KafkaConfig.getProducer();
    private static Map<String, MsgReceiver> topicThreadMap = new HashMap<String, MsgReceiver>();
    private static ArrayList<String> topicList = new ArrayList<String>();
    private static ChatClient client;
    private static Consumer<String, ArrayList<String>> serverArrayConsumer = KafkaConfig.getArrayConsumer(userId);


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
                        pickUser();
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
        System.out.println("4. Message user");
        System.out.println("5.Exit");
    }

    private static void listTopics() {

    }

    private static void pickUser() {

    }

    private static void pickTopic() {
        listTopics();
        System.out.println("Please write an existing topic name or a new one");
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(System.in));
        try {
            String option = reader.readLine();
            int topic = Integer.parseInt(option);
            String topicToAdd = reader.readLine();
            ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConstants.NICKNAMES_TOPIC, "SUBSCRIBE/"+topicToAdd+"*"+nickname);
            kafkaProducer.send(record);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private static void listUsers() {

    }
}
