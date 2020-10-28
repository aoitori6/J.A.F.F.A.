package authserver;

import java.net.InetSocketAddress;
import java.util.ArrayList;

public class InterfaceAuthServer {
    public static void main(String[] args) {
        try {
            ArrayList<InetSocketAddress> fileServersList = new ArrayList<InetSocketAddress>(1);
            fileServersList.add(new InetSocketAddress("localhost", 9696));
            AuthServer serverOne = new AuthServer(fileServersList,null);
            System.out.println("Port: " + serverOne.getServerPort());
            serverOne.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}