package authserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AuthServer {
    private final static byte NTHREADS = 12;
    private final ExecutorService clientThreadPool;
    private final ServerSocket authServer;

    private final static String url = "jdbc:mysql://localhost:3306/client_database";
    private Connection clientDB;
    private Socket primaryFileServerSocket;

    private ArrayList<InetSocketAddress> fileServersList = new ArrayList<InetSocketAddress>(1);
    private HashMap<InetSocketAddress, Thread> fileServers = new HashMap<InetSocketAddress, Thread>(1);

    /**
     * Constructor that automatically starts the Auth Server as a localhost and
     * listens on a random port. The server doesn't begin accepting requests until
     * the start method is called.
     * 
     * @param fileServersList List of File Servers the Auth Server will monitor and
     *                        maintain Sockets to
     * @throws IOException  If Server couldn't be initialized
     * @throws SQLException If Server couldn't establish a connection to the MySQL
     *                      DB
     */
    public AuthServer(ArrayList<InetSocketAddress> fileServersList) throws IOException, SQLException {
        // Initialize Auth Server to listen on some random port
        authServer = new ServerSocket(0);

        // Thread Pool to allocate Tasks to
        clientThreadPool = Executors.newFixedThreadPool(NTHREADS);

        // Client Database to authenticate against
        this.clientDB = DriverManager.getConnection(url, "root", "85246");

        // Try to connect to the Primary File Server
        try{
            this.primaryFileServerSocket = new Socket("localhost", 12609);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Attempt to establish connections
        for (InetSocketAddress fileServer : fileServersList) {
            try {
                fileServers.put(fileServer, new Thread(new AuthServerVerifyHandle(
                        new Socket(fileServer.getHostName(), fileServer.getPort()), this.clientDB)));
                this.fileServersList.add(fileServer);
            } catch (Exception e) {
                e.printStackTrace();
                System.err.format("ERROR: Couldn't connect to File Server %s! Ignoring.%n", fileServer.toString());
            }
        }

        // Starting File Server listeners
        for (InetSocketAddress fileServer : this.fileServersList) {
            fileServers.get(fileServer).start();
            System.err.format("INFO: Started Thread listened to File Server %s.%n", fileServer);
        }
    }

    public void start() {
        if (authServer.equals(null))
            throw new NullPointerException("Error. Auth Server was not initialized!");

        // Begin listening for new Socket connections
        while (!clientThreadPool.isShutdown()) {
            try {
                clientThreadPool.execute(new AuthServerHandler(authServer.accept(), clientDB, primaryFileServerSocket));
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        clientThreadPool.shutdown();
    }

    /**
     * Gets the port number associated with the AuthServer object
     * 
     * @return Port number the object is listening on
     */
    public int getServerPort() {
        return this.authServer.getLocalPort();
    }
}
