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

import server.fileserver.primary.PrimaryFileServer;

final public class InterfacePrimaryFileServer {
    private final static String DEFAULT_MYSQLDB_URL = "jdbc:mysql://localhost:3306/file_database";
    private final static String DEFAULT_MYSQLDB_USERNAME = "root";
    private final static String DEFAULT_MYSQLDB_PASSWORD = "85246";
    private final static Path DEFAULT_FILESTORAGEFOLDER_PATH = Paths.get(System.getProperty("user.home"),
            "sharenow_primarydb");

    private final static String DEFAULT_AUTH_IP = "localhost";
    private final static int DEFAULT_AUTH_PORT = 9000;

    public static void main(String[] args) {
        /*
         * Scanner conInput = new Scanner(System.in);
         * 
         * System.out.println("Welcome to the Primary File Server!");
         * 
         * // MySQL DB Input Connection fileDB; while (true) {
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
         * ); String path = conInput.nextLine();
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
         * // Auth Server address InetSocketAddress toAuth; while (true) { System.out.
         * println("Enter IP Address of Auth Server (Press Enter for default address)");
         * String ipAddr = conInput.nextLine(); if (ipAddr.equals("")) ipAddr =
         * DEFAULT_AUTH_IP;
         * System.out.println("Enter Port of Auth Server (Input 0 for default address)"
         * ); int port = conInput.nextInt(); if (port == 0) port = DEFAULT_AUTH_PORT;
         * conInput.nextLine(); try { toAuth = new InetSocketAddress(ipAddr, port);
         * break; } catch (Exception e) { e.printStackTrace();
         * System.out.println("ERROR! Address was invalid. Try again!"); continue; } }
         * 
         * System.out.println("Attempting to initalize the File Server");
         * PrimaryFileServer serverOne; try { serverOne =
         * PrimaryFileServer.getServer(fileDB, storagePath, toAuth); } catch
         * (IOException | SQLException e) { e.printStackTrace(); System.out.
         * println("ERROR! Couldn't initalize the Primary File Server! Shutting down");
         * conInput.close(); return; }
         * 
         * System.out.println("Listening for Replica Connections on: " +
         * serverOne.getReplicaPort());
         * System.out.println("Listening for Auth Connections on: " +
         * serverOne.getAuthPort());
         * 
         * try { serverOne.start(); } catch (IOException | SQLException e) {
         * e.printStackTrace();
         * System.out.println("ERROR! Exception during Primary Server's lifespan"); }
         * 
         * conInput.close();
         */

        // FOR TESTING PURPOSES ONLY
        PrimaryFileServer serverOne;
        try {
            Connection fileDB = DriverManager.getConnection(DEFAULT_MYSQLDB_URL, DEFAULT_MYSQLDB_USERNAME,
                    DEFAULT_MYSQLDB_PASSWORD);
            InetSocketAddress toAuth = new InetSocketAddress(DEFAULT_AUTH_IP, DEFAULT_AUTH_PORT);
            serverOne = PrimaryFileServer.getServer(fileDB, DEFAULT_FILESTORAGEFOLDER_PATH, toAuth);

            System.out.println("Listening for Replica Connections on: " + serverOne.getReplicaPort());
            System.out.println("Listening for Auth Connections on: " + serverOne.getAuthPort());

            serverOne.start();

        } catch (IOException | SQLException e) {
            e.printStackTrace();
            System.out.println("ERROR! Couldn't initalize the Primary File Server! Shutting down");
            return;
        }
    }
}