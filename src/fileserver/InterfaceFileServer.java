package fileserver;

public class InterfaceFileServer {
    public static void main(String[] args) {
        try {
            FileServer serverOne = new FileServer();
            System.out.println("Port: " + serverOne.getServerPort());
            serverOne.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}