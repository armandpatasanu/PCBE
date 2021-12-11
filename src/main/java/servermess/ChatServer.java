package servermess;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;

public class ChatServer {

    private final static Consumer<Long, String> consumer = KafkaConfig.getServerConsumer();
    private static final Producer<Long, String> kafkaProducer = KafkaConfig.getProducer();
    private static final Logger LOGGER = LoggerFactory.getLogger(Messagereceiver.class);
    private static ArrayList<User> users = new ArrayList<>();

    private static final int GIVE_UP = 100;

    public static void main(String[] args) {
        Thread user = new Thread(){
            public void run()
            {
                TopicUtils topicCreator = new TopicUtils();
                topicCreator.createTopic(KafkaConstants.NICKNAMES_TOPIC, "NO");
                consumer.subscribe(Collections.singleton(KafkaConstants.NICKNAMES_TOPIC));
                handleMessages();
                //Messagereceiver m = new Messagereceiver();
                //m.consumeMessage();
            }
        };
        user.start();
    }
//verif ca nu exista deja topicul

    public static void handleMessages() {
        int recordsCount = 0;

        while (true) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<Long, String> record : consumerRecords) {
                String command = record.value();
                if (command.startsWith("NICK/")) {
                    String commandRemoved = command.substring(5);
                    String parts[] = commandRemoved.split("\\*");
                    String nickname = parts[0];
                    String userId = parts[1];
                    User user = new User(nickname, UUID.fromString(userId));
                    users.add(user);
                    System.out.println(user.getNickname());
                    System.out.println("User:" + userId);
                    String topic = KafkaConstants.SERVER_CLIENT_TOPIC + "-" + nickname;
                    System.out.println("Topic" + topic);
                    ProducerRecord<Long, String> response = new ProducerRecord<>(topic, "User was created successfully!");
                    kafkaProducer.send(response);
                }
            }

            /*consumerRecords.forEach(consumerRecord -> LOGGER.info("Consumer Record:({} {})",
                    consumerRecord.key(), consumerRecord.value(),
                    consumerRecord.partition(), consumerRecord.offset()));*/

            //consumer.commitSync();


        //consumer.close();

        LOGGER.info("Done");
        }
    }
}
