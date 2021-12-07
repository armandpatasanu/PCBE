package servermess;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class ChatClient {

    private static boolean isLoggedIn = false;
    //private static final Logger LOGGER = LoggerFactory.getLogger(Messagereceiver.class);

    private static final int GIVE_UP = 100;
    private static final UUID userId = UUID.randomUUID();
    private static final Consumer<Long, String> consumer = KafkaConfig.getConsumer(userId);
    private static final Producer<Long, String> kafkaProducer = KafkaConfig.getProducer();
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

    public static void main(String[] args) {
        Thread chat = new Thread(){
            public void run()
            {
                do {
                    consumer.subscribe(Collections.singleton(KafkaConstants.SERVER_CLIENT_TOPIC));
                    System.out.println("Please pick a nickname:");
                    BufferedReader reader = new BufferedReader(
                            new InputStreamReader(System.in));
                    try {
                        String nickname = reader.readLine();
                        if (nickname.length()!=0 && nickname != null)
                        {
                            isLoggedIn = true;
                            requestUserCreation(nickname, userId);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }while (!isLoggedIn);
                /*Messagereceiver m = new Messagereceiver();
                m.consumeMessageClient();*/
                MsgReceiver m = new MsgReceiver(consumer);
                m.start();
                topicThreadMap.put(KafkaConstants.SERVER_CLIENT_TOPIC, m);
                startChat();
            }
        };
        chat.start();
    }

    public static void startChat()
    {
        printMenu();
        boolean stop = false;
        do{
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(System.in));
            try {
                String option = reader.readLine();
                switch (option)
                {
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

        }while(!stop);
    }

    private static void pickUser() {

    }

    private static void pickTopic() {

    }

    private static void listUsers() {

    }

    private static void listTopics() {

        for (String topic:topicList) {
            System.out.println(topic);
        }
    }

    public static void printMenu()
    {
        System.out.println("Select the option number:");
        System.out.println("1. List topics");
        System.out.println("2. List online users");
        System.out.println("3. Join topic");
        System.out.println("4. Message user");
        System.out.println("5.Exit");
    }

    public static void requestUserCreation(String nickname, UUID userId)
    {
        ProducerRecord<Long, String> userRecord = new ProducerRecord<>(KafkaConstants.NICKNAMES_TOPIC, "NICK/"+nickname+"*"+userId.toString());
        kafkaProducer.send(userRecord);
    }

    public static void consumeMessages()
    {
        int recordsCount = 0;
        while (true) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

            if (consumerRecords.count() == 0) {
                recordsCount++;
                if (recordsCount > GIVE_UP) {
                    break;
                } else {
                    continue;
                }
            }
            for (ConsumerRecord<Long, String> record : consumerRecords)
            {

            }

            /*consumerRecords.forEach(consumerRecord -> LOGGER.info("Consumer Record:({} {})",
                    consumerRecord.key(), consumerRecord.value(),
                    consumerRecord.partition(), consumerRecord.offset()));*/

            consumer.commitAsync();
        }

        consumer.close();
        //LOGGER.info("Done");
    }

    public void consumeMessageClient(){

        int recordsCount = 0;

        while (true) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

            if (consumerRecords.count() == 0) {
                recordsCount++;
                if (recordsCount > GIVE_UP) {
                    break;
                } else {
                    continue;
                }
            }
            for (ConsumerRecord<Long, String> record : consumerRecords)
            {

            }

            /*consumerRecords.forEach(consumerRecord -> LOGGER.info("Consumer Record:({} {})",
                    consumerRecord.key(), consumerRecord.value(),
                    consumerRecord.partition(), consumerRecord.offset()));*/

            consumer.commitAsync();
        }

        consumer.close();
        //LOGGER.info("Done");
    }
}
