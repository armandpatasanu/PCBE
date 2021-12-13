package servermess;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
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

    public void createTopic(String topic)
    {
        Properties properties = new Properties();
        properties.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
        //Admin admin = Admin.create(properties);

        try (Admin admin = Admin.create(properties)) {
            int partitions = 10;
            short replicationFactor = 1;
            NewTopic newTopic = new NewTopic(topic, partitions, replicationFactor);

            CreateTopicsResult result = admin.createTopics(
                    Collections.singleton(newTopic)
            );

            KafkaFuture<Void> future = result.values().get(topic);
            future.get();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String getTopics() throws ExecutionException, InterruptedException {

        String topics = "";
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (Admin adminClient = Admin.create(properties)) {

            ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
            listTopicsOptions.listInternal(true);

            Set<String> set =  adminClient.listTopics(listTopicsOptions).names().get();
            for (String s:set)
            {
                if(s.startsWith(KafkaConstants.TOPICS_TOPIC))
                    topics = topics + "*" + s;
            }
            return topics;
        }
        catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }
}
