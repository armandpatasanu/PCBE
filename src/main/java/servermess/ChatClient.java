package servermess;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import static org.apache.kafka.common.utils.Utils.sleep;


public class ChatClient implements Runnable{


    //private static final Logger LOGGER = LoggerFactory.getLogger(Messagereceiver.class);

    private static final int GIVE_UP = 100;
    private static final Producer<String, String> kafkaProducer = KafkaConfig.getProducer();
    private final UUID userId;
    private final String topic;
    private final String nickname;
    private MsgReceiver m;

    public ChatClient(UUID userId, String topic, String nickname)
    {
        this.userId = userId;
        this.topic = topic;
        this.nickname = nickname;
        TopicUtils topicCreator = new TopicUtils();
        if (!topicCreator.checkTopicExist(topic))
            topicCreator.createTopic(KafkaConstants.SERVER_CLIENT_TOPIC, nickname);
        m = new MsgReceiver(userId, topic);
        m.start();
        requestUserCreation(nickname, userId);
    }

    public static void requestUserCreation(String nickname, UUID userId) {
        ProducerRecord<String, String> userRecord = new ProducerRecord<>(KafkaConstants.NICKNAMES_TOPIC, "NICK/" + nickname + "*" + userId.toString());
        kafkaProducer.send(userRecord);
    }

    public void pingServer()
    {
        try{

            while (true){
                sleep(100);
                ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConstants.PING_TOPIC,"Ping de la user " + nickname); //USERID * actualtime
                kafkaProducer.send(record);

            }
        }finally {
            kafkaProducer.close();
        }
    }

    public void run() {
        pingServer();
        System.out.println("Client has started");
    }

}
