package servermess;

public class KafkaConstants {
    private KafkaConstants(){}

    public static final String  NICKNAMES_TOPIC = "kafka-nicknames-topic";
    public static final String SERVER_CLIENT_TOPIC = "kafka-server-client";
    public static final String USER_TOPIC = "kafka-user";
    public static final String TOPICS_TOPIC = "kafka-topics-topic";
    public static final String CHECK_NICK = "kafka-check-nick";
    // Kafka Brokers
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String PING_TOPIC = "kafka-ping";
}
