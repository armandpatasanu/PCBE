package servermess;

public class KafkaConstants {
    private KafkaConstants(){}

    public static final String TOPIC = "kafka-java-topic";
    public static final String  NICKNAMES_TOPIC = "kafka-nicknames-topic";
    public static final String SERVER_CLIENT_TOPIC = "kafka-server-client";
    public static final String USERS_TOPIC = "kafka-users";
    public static final String FETCHTOPICS_TOPIC = "kafka-fetchtopics-topic";
    public static final String TOPICS_TOPIC = "kafka-topics-topic";
    public static final String FETCHUSERS_TOPIC = "kafka-users-topic";
    // Kafka Brokers
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String PING_TOPIC = "kafka-ping";
}
