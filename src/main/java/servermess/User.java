package servermess;

import org.apache.kafka.common.protocol.types.Field;

import java.security.PublicKey;
import java.util.ArrayList;
import java.util.UUID;

public class User {
    private UUID userId;
    private String nickname;
    private ArrayList<String> topics;

    public User(String nickname, UUID userId)
    {
        this.userId = userId;
        this.nickname = nickname;
    }

    public String getNickname()
    {
        return this.nickname;
    }

    public void setNickname(String nickname)
    {
        this.nickname = nickname;
    }

    public void addTopic(String topicName)
    {
        topics.add(topicName);
    }

    public void removeTopic(String topicName)
    {
        topics.remove(topicName);
    }

    public void listTopics()
    {
        for (String topic : topics)
        {
            System.out.println(topic);
        }
    }

    public UUID getUserId()
    {
        return this.userId;
    }
}
