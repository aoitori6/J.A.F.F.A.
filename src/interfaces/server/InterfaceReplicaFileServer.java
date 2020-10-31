package interfaces.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Scanner;

import server.fileserver.replica.ReplicaFileServer;

public class InterfaceReplicaFileServer {
    private final static String DEFAULT_MYSQLDB_URL = "jdbc:mysql://localhost:3306/file_database";
    private final static String DEFAULT_MYSQLDB_USERNAME = "root";
    private final static String DEFAULT_MYSQLDB_PASSWORD = "85246";
    private final static Path DEFAULT_FILESTORAGEFOLDER_PATH = Paths.get(System.getProperty("user.home"),
            "sharenow_replicadb");

    private final static String DEFAULT_AUTHSERVICE_IP = "localhost";
    private final static int DEFAULT_AUTHSERVICE_PORT = 10000;

    private final static String DEFAULT_PRIMARY_IP = "localhost";
    private final static int DEFAULT_PRIMARY_PORT = 12600;

    public static void main(String[] args) {
        /*
         * Scanner conInput = new Scanner(System.in);
         * 
         * System.out.println("Welcome to the Replica File Server!");
         * 
         * /// MySQL DB Input Connection fileDB; while (true) {
         * System.out.println("Enter MySQL File DB Address (Press Enter for default)");
         * String dbAddr = conInput.nextLine();
         * 
         * if (dbAddr.equals("")) dbAddr = DEFAULT_MYSQLDB_URL;
         * 
         * System.out.
         * println("Enter MySQL File DB Credentials (Press Enter for default creds)");
         * System.out.print("Name: "); String dbUser = conInput.nextLine();
         * System.out.print("%sPassword: "); String dbPass = conInput.nextLine();
         * 
         * if (dbUser.equals("")) dbUser = DEFAULT_MYSQLDB_USERNAME;
         * 
         * if (dbPass.equals("")) dbPass = DEFAULT_MYSQLDB_PASSWORD;
         * 
         * try { fileDB = DriverManager.getConnection(dbAddr, dbUser, dbPass); break; }
         * catch (SQLException e) { e.printStackTrace(); System.out.
         * println("ERROR! Either DB URL was invalid or credentials were. Try again!");
         * continue; } }
         * 
         * // File System storage folder Path storagePath; while (true) {
         * System.out.println(
         * "Enter Path to folder where Uploaded files will be stored (as \\codename\\file) (Press Enter for default)"
         * );
         * 
         * String path = conInput.nextLine();
         * 
         * if (path.equals("")) { storagePath = DEFAULT_FILESTORAGEFOLDER_PATH; try {
         * Files.createDirectories(storagePath); } catch (IOException e) {
         * e.printStackTrace();
         * System.out.println("ERROR! Couldn't create default folders. Try again!");
         * continue; } break; }
         * 
         * else if (Files.exists(Paths.get(path))) System.out.
         * println("ERROR! Either Path was invalid or non-existent. Try again!");
         * continue; }
         * 
         * // Auth Service address InetSocketAddress toAuthService; while (true) {
         * System.out.
         * println("Enter IP Address of Auth Service (Press Enter for default address)"
         * ); String ipAddr = conInput.nextLine(); if (ipAddr.equals("")) ipAddr =
         * DEFAULT_AUTHSERVICE_IP;
         * System.out.println("Enter Port of Auth Server (Input 0 for default address)"
         * ); int port = conInput.nextInt(); if (port == 0) port =
         * DEFAULT_AUTHSERVICE_PORT; conInput.nextLine(); try { toAuthService = new
         * InetSocketAddress(ipAddr, port); break; } catch (Exception e) {
         * e.printStackTrace();
         * System.out.println("ERROR! Address was invalid. Try again!"); continue; } }
         * 
         * // Primary File Server address InetSocketAddress toPrimaryServer; while
         * (true) { System.out.
         * println("Enter IP Address of Auth Service (Press Enter for default address)"
         * ); String ipAddr = conInput.nextLine(); if (ipAddr.equals("")) ipAddr =
         * DEFAULT_PRIMARY_IP;
         * System.out.println("Enter Port of Auth Server (Input 0 for default address)"
         * ); int port = conInput.nextInt(); if (port == 0) port = DEFAULT_PRIMARY_PORT;
         * conInput.nextLine(); try { toPrimaryServer = new InetSocketAddress(ipAddr,
         * port); break; } catch (Exception e) { e.printStackTrace();
         * System.out.println("ERROR! Address was invalid. Try again!"); continue; } }
         * 
         * System.out.println("Attempting to initalize the File Server");
         * ReplicaFileServer serverOne; try { serverOne =
         * ReplicaFileServer.getServer(fileDB, storagePath, toAuthService,
         * toPrimaryServer); } catch (IOException | SQLException e) {
         * e.printStackTrace(); System.out.
         * println("ERROR! Couldn't initalize the Replica File Server! Shutting down");
         * conInput.close(); return; }
         * 
         * System.out.println("Listening for Client Connections on: " +
         * serverOne.getServerPort());
         * 
         * try { serverOne.start(); } catch (IOException | SQLException e) {
         * e.printStackTrace();
         * System.out.println("ERROR! Exception during Replica Server's lifespan"); }
         * 
         * conInput.close();
         */

        // FOR TESTING PURPOSES ONLY
        try {
            Connection fileDB = DriverManager.getConnection(DEFAULT_MYSQLDB_URL, DEFAULT_MYSQLDB_USERNAME,
                    DEFAULT_MYSQLDB_PASSWORD);
            final InetSocketAddress authServiceAddr = new InetSocketAddress("localhost", 10000);
            final InetSocketAddress primaryServerAddr = new InetSocketAddress("localhost", 12600);
            ReplicaFileServer serverOne = ReplicaFileServer.getServer(fileDB, DEFAULT_FILESTORAGEFOLDER_PATH,
                    authServiceAddr, primaryServerAddr);
            System.out.println("Port: " + serverOne.getServerPort());
            serverOne.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}