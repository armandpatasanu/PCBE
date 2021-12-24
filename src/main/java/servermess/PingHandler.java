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
    private static AbstractMap<String, Long> onlineUsers;

    public PingHandler(AbstractMap<String, Long> users) {
        consumer = KafkaConfig.getServerConsumer();
        consumer.subscribe(Collections.singleton(KafkaConstants.PING_TOPIC));
        this.onlineUsers = users;
    }


    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    String nickname = record.value().substring(5);
                    if(!onlineUsers.containsKey(nickname))
                    {
                        onlineUsers.put(nickname,record.timestamp());
                    }
                    for (String user : onlineUsers.keySet()){
                        if (user.equals(nickname)){
                            onlineUsers.replace(user,record.timestamp());
                        }
                    }

                }
                for (String user : onlineUsers.keySet()) {
                    if (System.currentTimeMillis() - onlineUsers.get(user) > MAX_INACTIVE_TIME)
                    {
                        onlineUsers.remove(user);
                        System.out.println(user + " logged out!");
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

}


