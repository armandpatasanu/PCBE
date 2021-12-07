package servermess;

public class KafkaConstants {
    private KafkaConstants(){}

    public static final String TOPIC = "kafka-java-topic";
    public static final String  NICKNAMES_TOPIC = "kafka-nicknames-topic";
    public static final String SERVER_CLIENT_TOPIC = "kafka-server-client";
    // Kafka Brokers
    public static final String BOOTSTRAP_SERVERS = "localhost:9092, localhost:9093, localhost:9094";
}
