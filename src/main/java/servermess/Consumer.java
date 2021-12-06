package servermess;

public class Consumer {
    public static void main(String[] args) {
        new Thread(new Messagereceiver()::consumeMessage).start();
    }
}
