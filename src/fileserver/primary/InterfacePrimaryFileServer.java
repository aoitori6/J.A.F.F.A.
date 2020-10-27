package fileserver.primary;

public class InterfacePrimaryFileServer {
    public static void main(String[] args) {
        try {
            PrimaryFileServer serverOne = new PrimaryFileServer(1);
            System.out.println("Listening for Replica Connections on: " + serverOne.getReplicaPort());
            System.out.println("Listening for Auth Connections on: " + serverOne.getAuthPort());
            serverOne.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}