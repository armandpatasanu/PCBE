package servermess;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TopicUtils {
    public TopicUtils() {
    }

    public boolean checkTopicExist(String topic) {
        Properties properties = new Properties();
        properties.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        //Admin admin = Admin.create(properties);

        try (Admin admin = Admin.create(properties)) {

          return admin.listTopics().names().get().stream().anyMatch(topicName -> topicName.equalsIgnoreCase(topic));
        } catch (ExecutionException ex) {
            ex.printStackTrace();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
        return false;
    }
    public void createTopic(String topic, String userId)
    {
        Properties properties = new Properties();
        properties.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        //Admin admin = Admin.create(properties);

        try (Admin admin = Admin.create(properties)) {
            int partitions = 10;
            short replicationFactor = 1;
            String topicName = "";
            if (!userId.equals("N0"))
                topicName =  topic+"-"+userId;
            else
                topicName = topic;
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);

            CreateTopicsResult result = admin.createTopics(
                    Collections.singleton(newTopic)
            );

            KafkaFuture<Void> future = result.values().get(topicName);
            future.get();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
