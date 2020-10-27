package fileserver.primary;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PrimaryFileServer {
    private final static byte NTHREADS = 30;
    private final ExecutorService threadPool;
    private final ScheduledExecutorService fileCleanup;

    private final ServerSocket authServerSocket;
    private final ServerSocket replicaServerSocket;
    private final static String url = "jdbc:mysql://localhost:3306/file_database";
    private Connection fileDB;

    private ArrayList<InetSocketAddress> replicaServers;
    private HashMap<InetSocketAddress, Socket> replicaListeners;

    /**
     * Constructor that automatically starts the Primary File Server as a localhost
     * and listens on a random port. The server doesn't begin accepting requests
     * until the start method is called.
     * 
     * @param replicaCount Number of Replica Servers the Primary File Server will
     *                     listen to
     * @throws IOException  If Server couldn't be initialized
     * @throws SQLException If Server couldn't establish a connection to the MySQL
     *                      DB
     */
    public PrimaryFileServer(int replicaCount) throws IOException, SQLException {
        // Initialize FIle Server to listen on some random port for Auth Server
        // connections
        this.authServerSocket = new ServerSocket(12609);

        // Initialize FIle Server to listen on some random port for Replica Server
        // connections
        this.replicaServerSocket = new ServerSocket(12600);

        // Thread Pool to allocate Tasks to
        this.threadPool = Executors.newFixedThreadPool(NTHREADS);

        // Scheduled Executor Service that will be responsible for cleaning up deleted
        // files
        this.fileCleanup = Executors.newSingleThreadScheduledExecutor();

        // Initialize connection to File Database
        // TODO: Get this DB from admin; Remove hardcoding
        this.fileDB = DriverManager.getConnection(url, "root", "85246");
        this.fileDB.setAutoCommit(false);

        this.replicaServers = new ArrayList<InetSocketAddress>(replicaCount);
        this.replicaListeners = new HashMap<InetSocketAddress, Socket>();
    }

    public void start() throws IOException, SQLException {
        if (authServerSocket.equals(null))
            throw new NullPointerException("Error. File Server was not initialized!");

        // Attempt to establish Connection to all Replica Servers
        Socket tempSockt;
        InetSocketAddress tempAddr;
        for (int i = 0; i < this.replicaServers.size(); ++i) {
            tempSockt = this.replicaServerSocket.accept();
            tempAddr = new InetSocketAddress(tempSockt.getInetAddress(), tempSockt.getLocalPort());
            this.replicaServers.add(tempAddr);
            this.replicaListeners.put(tempAddr, tempSockt);
        }

        // Start File Cleanup thread
        try {
            fileCleanup.scheduleWithFixedDelay(new DeletionService(), 0, 20, TimeUnit.MINUTES);
        } catch (Exception e) {
            e.printStackTrace();
            // Try to restart the Cleanup thread
            fileCleanup.scheduleWithFixedDelay(new DeletionService(), 0, 20, TimeUnit.MINUTES);
        }

        // Begin listening for new Auth Server connections
        while (!threadPool.isShutdown()) {
            try {
                threadPool.execute(new FromAuthHandler(this.authServerSocket.accept(), this.fileDB));
            } catch (IOException e) {
                e.printStackTrace();
                continue;
            }

        }

        this.authServerSocket.close();
        this.fileDB.close();
        this.threadPool.shutdown();
    }

    /**
     * Gets the port number associated with the authServerSocket object
     * 
     * @return Port number the object is listening on
     */
    public int getAuthPort() {
        return this.authServerSocket.getLocalPort();
    }

    /**
     * Gets the port number associated with the replicaServerSocket object
     * 
     * @return Port number the object is listening on
     */
    public int getReplicaPort() {
        return this.replicaServerSocket.getLocalPort();
    }
}
