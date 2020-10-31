package fileserver.replica;

public class InterfaceReplicaFileServer {
    public static void main(String[] args) {
        try {
            ReplicaFileServer serverOne = new ReplicaFileServer();
            System.out.println("Port: " + serverOne.getServerPort());
            serverOne.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}