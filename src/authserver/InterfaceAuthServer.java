package authserver;

import java.io.IOException;

public class InterfaceAuthServer {
    public static void main(String[] args) {
        try {
            AuthServer serverOne = new AuthServer();
            System.out.println("Port: "+serverOne.getServerPort());
            serverOne.start();
        } catch (Exception e) {
            e.printStackTrace();
        }   
    }
}