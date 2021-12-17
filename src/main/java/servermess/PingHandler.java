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
    private final Map<User, Long> onlineUsers;
    private long timestamp = 0;
    //private final ConcurrentHashMap<User, Long> loggedUsers = new ConcurrentHashMap<>();   //memoram userID si timestamp

    public PingHandler(Map<User, Long> users) {
        consumer = KafkaConfig.getServerConsumer();
        consumer.subscribe(Collections.singleton(KafkaConstants.PING_TOPIC));
        this.onlineUsers = users;  //unde eu adaug nickname si timestapul ??????
    }


    public void run() {
        try {
            while (true) {
                //System.out.printf("Current time = %d", now);
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String nickname = record.value().substring(11);
;
                    for (User user : onlineUsers.keySet()){
                        if (user.getNickname().equals(nickname)){
                            onlineUsers.replace(user,record.timestamp()); ///e bine oare??
                        }
                    }
                    for(User user : onlineUsers.keySet()) {
                        System.out.println("key: " + user.getNickname() + "value: " + onlineUsers.get(user));
                    }
//                    //daca e inactiv
//                    if(currentTime - record.timestamp() < MAX_INACTIVE_TIME){
//                        loggedUsers.remove(user.getUserId());   //ar trebui sa fie userID
//                        ProducerRecord<String, String> logOut = new ProducerRecord<>(KafkaConstants.PING_TOPIC,"User-ul cu ID-ul " + user.getUserId() + "nu mai e activ");
//                    }

                    //System.out.printf("offset = %d, key = %s, value = %s%n topic = %s%n", record.offset(), record.key(), record.value(), record.topic());

                }
            }
        } finally {
            consumer.close();
        }
    }

}


