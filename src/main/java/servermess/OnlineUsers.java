package servermess;

import servermess.User;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class OnlineUsers {
    private final Map<String, Long> onlineUsers;

    public OnlineUsers(){
        onlineUsers = new ConcurrentHashMap<>();
    }

    public void addUser(String key, Long value){
        onlineUsers.put(key, value);
    }
    public void deleteUser(String user){
        onlineUsers.remove(user);
    }
    public Map<String, Long> getOnlineUser(){
        return onlineUsers;
    }
    public void replaceValue(String key, Long value){
        onlineUsers.replace(key, value);
    }
}
