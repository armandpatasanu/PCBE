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
    private static final int MAX_INACTIVE_TIME = 5000;
    private final AbstractMap<String, Long> onlineUsers;
    //private final ConcurrentHashMap<User, Long> loggedUsers = new ConcurrentHashMap<>();   //memoram userID si timestamp

    public PingHandler(AbstractMap<String, Long> users) {
        consumer = KafkaConfig.getServerConsumer();
        consumer.subscribe(Collections.singleton(KafkaConstants.PING_TOPIC));
        this.onlineUsers = users;  //unde eu adaug nickname si timestapul ??????
    }


    public void run() {
        try {
            while (true) {
                //System.out.printf("Current time = %d", now);
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    String nickname = record.value().substring(5);
                    //System.out.println(nickname);
                    if(!onlineUsers.containsKey(nickname))
                    {
                        System.out.println("I am here" + nickname);
                        onlineUsers.put(nickname,record.timestamp());
                    }
                    for (String user : onlineUsers.keySet()){
                        if (user.equals(nickname)){
                            //System.out.println(record.timestamp());
                            onlineUsers.replace(user,record.timestamp()); ///e bine oare??
                        }
                    }
                    for(String user : onlineUsers.keySet()) {
                        System.out.println("{key: " + user + ", value: " + onlineUsers.get(user) + "}");
                    }
                    //daca e inactiv
                    //System.out.printf("offset = %d, key = %s, value = %s%n topic = %s%n", record.offset(), record.key(), record.value(), record.topic());

                }
                for (String user : onlineUsers.keySet()) {
                    if (System.currentTimeMillis() - onlineUsers.get(user) > MAX_INACTIVE_TIME)
                    {
                        onlineUsers.remove(user);   //ar trebui sa fie userID
                        System.out.println(user + " logged out!");
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

}


