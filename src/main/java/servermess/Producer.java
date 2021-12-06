package servermess;

public class Producer {
    public static void main( String[] args ) {
        new Thread(() -> new Messagereceiver().sendMessage(25)).start();
    }
}
