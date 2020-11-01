package interfaces.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Scanner;

import server.authserver.AuthServer;

public class InterfaceAuthServer {
    private final static String DEFAULT_MYSQLDB_URL = "jdbc:mysql://localhost:3306/client_database";
    private final static String DEFAULT_MYSQLDB_USERNAME = "root";
    private final static String DEFAULT_MYSQLDB_PASSWORD = "85246";

    private final static String DEFAULT_PRIMARY_IP = "localhost";
    private final static int DEFAULT_PRIMARY_PORT = 12609;
    private static final String DEFAULT_REPLICA_IP = "localhost";
    private static final int DEFAULT_REPLICA_PORT = 7689;

    public static void main(String[] args) {

        /*
         * Scanner conInput = new Scanner(System.in);
         * 
         * System.out.println("Welcome to the Auth Server!");
         * 
         * // MySQL DB Input Connection clientDB; while (true) {
         * System.out.println("Enter MySQL Client DB Address (Press Enter for default)"
         * ); String dbAddr = conInput.nextLine();
         * 
         * if (dbAddr.equals("")) dbAddr = DEFAULT_MYSQLDB_URL;
         * 
         * System.out.
         * println("Enter MySQL Client DB Credentials (Press Enter for default creds)");
         * System.out.print("Name: "); String dbUser = conInput.nextLine();
         * System.out.print("%sPassword: "); String dbPass = conInput.nextLine();
         * 
         * if (dbUser.equals("")) dbUser = DEFAULT_MYSQLDB_USERNAME;
         * 
         * if (dbPass.equals("")) dbPass = DEFAULT_MYSQLDB_PASSWORD;
         * 
         * try { clientDB = DriverManager.getConnection(dbAddr, dbUser, dbPass); break;
         * } catch (SQLException e) { e.printStackTrace(); System.out.
         * println("ERROR! Either DB URL was invalid or credentials were. Try again!");
         * continue; } }
         * 
         * // Primary File Server address InetSocketAddress toPrimary; while (true) {
         * System.out.
         * println("Enter IP Address of Primary File Server (Press Enter for default address)"
         * ); String ipAddr = conInput.nextLine(); if (ipAddr.equals("")) ipAddr =
         * DEFAULT_PRIMARY_IP; System.out.
         * println("Enter Port of Primary File Server (Input 0 for default port)"); int
         * port = conInput.nextInt(); if (port == 0) port = DEFAULT_PRIMARY_PORT;
         * conInput.nextLine(); try { toPrimary = new InetSocketAddress(ipAddr, port);
         * break; } catch (Exception e) { e.printStackTrace();
         * System.out.println("ERROR! Address was invalid. Try again!"); continue; } }
         * 
         * // Replica File Server addresses HashMap<InetSocketAddress,
         * InetSocketAddress> replicaAddrs = new HashMap<InetSocketAddress,
         * InetSocketAddress>(); InetSocketAddress forAuth; InetSocketAddress forClient;
         * while (true) { System.out.println(
         * "Enter IP Address of Replica File Server with respect to Auth (Press Enter for default address)"
         * ); String ipAddrAuth = conInput.nextLine();
         * 
         * if (ipAddrAuth.equals("")) ipAddrAuth = DEFAULT_PRIMARY_IP;
         * 
         * System.out.
         * println("Enter Port of Replica File Server with respect to Auth (Input 0 for default port)"
         * ); int portAuth = conInput.nextInt(); conInput.nextLine();
         * 
         * if (portAuth == 0) portAuth = DEFAULT_PRIMARY_PORT;
         * 
         * try { forAuth = new InetSocketAddress(ipAddrAuth, portAuth); } catch
         * (Exception e) { e.printStackTrace();
         * System.out.println("ERROR! Address was invalid. Try again!"); continue; }
         * 
         * System.out.println(
         * "Enter IP Address of Replica File Server with respect to Client (Press Enter for default address)"
         * ); String ipAddrClient = conInput.nextLine();
         * 
         * if (ipAddrClient.equals("")) ipAddrClient = DEFAULT_REPLICA_IP;
         * 
         * System.out.
         * println("Enter Port of Replica File Server with respect to Client (Input 0 for default port)"
         * ); int portClient = conInput.nextInt(); conInput.nextLine();
         * 
         * if (portClient == 0) portClient = DEFAULT_REPLICA_PORT;
         * 
         * try { forClient = new InetSocketAddress(ipAddrClient, portClient); } catch
         * (Exception e) { e.printStackTrace();
         * System.out.println("ERROR! Address was invalid. Try again!"); continue; }
         * 
         * replicaAddrs.put(forAuth, forClient);
         * 
         * System.out.println("Enter 1 to exit the loop"); int _temp_ =
         * conInput.nextInt(); conInput.nextLine();
         * 
         * if (_temp_ == 1) break; }
         * 
         * System.out.println("Attempting to initalize the File Server"); AuthServer
         * serverOne; try { serverOne = AuthServer.getServer(clientDB, toPrimary,
         * replicaAddrs); } catch (IOException | SQLException e) { e.printStackTrace();
         * System.out.
         * println("ERROR! Couldn't initalize the Primary File Server! Shutting down");
         * conInput.close(); return; }
         * 
         * System.out.println("Listening for Clients on: " + serverOne.getServerPort());
         * System.out.println("Listening for Auth Service requests on: " +
         * serverOne.getAuthServicePort());
         * 
         * try { serverOne.start(); } catch (IOException | SQLException |
         * InterruptedException e) { e.printStackTrace();
         * System.out.println("ERROR! Exception during Auth Server's lifespan");
         * serverOne.shutDown(); }
         * 
         * conInput.close();
         */

        // FOR TESTING PURPOSES ONLY
        try {
            HashMap<InetSocketAddress, InetSocketAddress> fileServersList = new HashMap<InetSocketAddress, InetSocketAddress>(
                    1);
            fileServersList.put(new InetSocketAddress("localhost", 7689), new InetSocketAddress("localhost", 7689));
            Connection clientDB = DriverManager.getConnection(DEFAULT_MYSQLDB_URL, DEFAULT_MYSQLDB_USERNAME,
                    DEFAULT_MYSQLDB_PASSWORD);

            AuthServer serverOne = AuthServer.getServer(clientDB,
                    new InetSocketAddress(DEFAULT_PRIMARY_IP, DEFAULT_PRIMARY_PORT), fileServersList);
            System.out.println("Main Port: " + serverOne.getServerPort());
            System.out.println("Auth Service Port: " + serverOne.getAuthServicePort());
            serverOne.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}