package authserver;

import java.net.InetSocketAddress;
import java.util.HashMap;

public class InterfaceAuthServer {
    public static void main(String[] args) {
        try {

            HashMap<InetSocketAddress, InetSocketAddress> fileServersList = new HashMap<InetSocketAddress, InetSocketAddress>(
                    1);
            fileServersList.put(new InetSocketAddress("localhost", 7689), new InetSocketAddress("localhost", 7689));

            AuthServer serverOne = new AuthServer(fileServersList);
            System.out.println("Main Port: " + serverOne.getServerPort());
            System.out.println("Auth Service Port: " + serverOne.getAuthServicePort());
            serverOne.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}