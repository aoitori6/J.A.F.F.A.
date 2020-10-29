package fileserver.primary;

import java.net.InetSocketAddress;

public class InterfacePrimaryFileServer {
    public static void main(String[] args) {
        try {
            PrimaryFileServer serverOne = new PrimaryFileServer(new InetSocketAddress("localhost", 9000), 1);
            System.out.println("Listening for Replica Connections on: " + serverOne.getReplicaPort());
            System.out.println("Listening for Auth Connections on: " + serverOne.getAuthPort());
            serverOne.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}