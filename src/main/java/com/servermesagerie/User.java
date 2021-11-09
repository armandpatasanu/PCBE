package com.servermesagerie;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class User {

    private String nickname;
    private UUID userId;
    private List<String> channelList = new ArrayList<String>();

    public User(String nickname)
    {
        this.nickname = nickname;
        this.userId = UUID.randomUUID();
    }

    public String getNickname() {
        return this.nickname;
    }

    public void setNickname(String nickname) {
        this.nickname = nickname;
    }

    public List<String> getChannelList() {
        return this.channelList;
    }

    public void addChannelToList(String channelName)
    {
        this.channelList.add(channelName);
    }
}
