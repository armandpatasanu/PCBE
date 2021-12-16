package servermess;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.*;


public class PingHandler extends Thread {

    private final Consumer<String, String> consumer;
    private static final int MAX_INACTIVE_TIME = 2000;
    private long currentTime = System.currentTimeMillis();
    private static ArrayList<User> my_users;
    private final ConcurrentHashMap<User, Long> loggedUsers = new ConcurrentHashMap<>();   //memoram userID si timestamp

    public PingHandler(ArrayList<User> users) {
        consumer = KafkaConfig.getServerConsumer();
        consumer.subscribe(Collections.singleton(KafkaConstants.PING_TOPIC));
        my_users = new ArrayList<>();  //unde eu adaug user ID si timestapul ??????
    }


    public void run() {
        try {
            Iterator<User> iterator = my_users.iterator();
            while(iterator.hasNext()) {
                User user = iterator.next();
                loggedUsers.put(user, 0L);
            }
            System.out.println("PRINTING1");
            for (User user: my_users) {
                System.out.println(user.getNickname());
                while (true) {
                    //System.out.printf("Current time = %d", now);
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    //System.out.println("Polling");
                    for (ConsumerRecord<String, String> record : records) {
                        String nickname = record.value().substring(11);
                        Iterator<User> it = my_users.iterator();

                        while (it.hasNext()) {
                            User user1 = it.next();
                            if (user1.getNickname().equals(nickname)) {
                                loggedUsers.replace(user1, record.timestamp());
                            }
                        }
                        System.out.println("PRINTING2");
                        //loggedUsers.forEach((k, v) -> System.out.println(k + ", " + v));
                        System.out.println("Lista " + loggedUsers);
//                    //daca e inactiv
//                    if(currentTime - record.timestamp() < MAX_INACTIVE_TIME){
//                        loggedUsers.remove(user.getUserId());   //ar trebui sa fie userID
//                        ProducerRecord<String, String> logOut = new ProducerRecord<>(KafkaConstants.PING_TOPIC,"User-ul cu ID-ul " + user.getUserId() + "nu mai e activ");
//                    }

                        //System.out.printf("offset = %d, key = %s, value = %s%n topic = %s%n", record.offset(), record.key(), record.value(), record.topic());

                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

}


