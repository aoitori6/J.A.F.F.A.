package authserver;

import java.net.InetSocketAddress;
import java.util.ArrayList;

public class InterfaceAuthServer {
    public static void main(String[] args) {
        try {
            ArrayList<InetSocketAddress> authRequesterAddrs = new ArrayList<InetSocketAddress>(1);
            authRequesterAddrs.add(new InetSocketAddress("localhost", 9696));
            ArrayList<InetSocketAddress> fileServersList = new ArrayList<InetSocketAddress>(1);
            fileServersList.add(new InetSocketAddress("localhost", 7689));
            AuthServer serverOne = new AuthServer(authRequesterAddrs, fileServersList);
            System.out.println("Port: " + serverOne.getServerPort());
            serverOne.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}