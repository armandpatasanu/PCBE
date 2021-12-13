package servermess;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ChatClientMain {

    private static boolean isLoggedIn = false;
    private static final UUID userId = UUID.randomUUID();
    private static Map<String, MsgReceiver> topicThreadMap = new HashMap<String, MsgReceiver>();
    private static ArrayList<String> topicList = new ArrayList<String>() {
        {
            add("football");
            add("f1");
            add("tennis");
            add("movies");
            add("music");
        }
    };

    public static void main(String[] args)
    {

        String nickname = "";
        String topic = "";
        do {


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
        ChatClient client = new ChatClient(userId, topic, nickname);
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

        for (String topic : topicList) {
            System.out.println(topic);
        }
    }

    private static void pickUser() {

    }

    private static void pickTopic() {

    }

    private static void listUsers() {

    }
}
