package authserver;

import java.net.InetSocketAddress;
import java.util.ArrayList;

public class InterfaceAuthServer {
    public static void main(String[] args) {
        try {
            ArrayList<InetSocketAddress> fileServersList = new ArrayList<InetSocketAddress>(1);
            fileServersList.add(new InetSocketAddress("localhost", 7689));

            AuthServer serverOne = new AuthServer(fileServersList);
            System.out.println("Main Port: " + serverOne.getServerPort());
            System.out.println("Auth Service Port: " + serverOne.getAuthServicePort());
            serverOne.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}