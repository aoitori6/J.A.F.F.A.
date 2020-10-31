package fileserver.primary;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PrimaryFileServer {
    private final static byte NTHREADS = 30;
    private final ExecutorService executionPool;
    private final ScheduledExecutorService fileCleanup;

    protected final static String SERVER_NAME = "Primary File Server";
    protected final static String SERVER_TOKEN = "JbaI6";
    protected final static Path FILESTORAGEFOLDER_PATH = Paths.get(System.getProperty("user.home"),
            "sharenow_primarydb");

    private final static String url = "jdbc:mysql://localhost:3306/file_database";
    private Connection fileDB;

    private final ServerSocket authServerSocket;
    private final ServerSocket replicaServerSocket;

    private InetSocketAddress authServerAddr;

    /**
     * Constructor that automatically starts the Primary File Server as a localhost
     * and listens on a random port. The server doesn't begin accepting requests
     * until the start method is called.
     * 
     * @param authServer   Address of the Auth Server responsible for syncing
     *                     Replicas
     * @param replicaCount Number of Replica Servers the Primary File Server will
     *                     listen to
     * @throws IOException  If Server couldn't be initialized
     * @throws SQLException If Server couldn't establish a connection to the MySQL
     *                      DB
     */
    public PrimaryFileServer(InetSocketAddress authServerAddr, int replicaCount) throws IOException, SQLException {
        // Initialize FIle Server to listen on some random port for Auth Server
        // connections
        this.authServerSocket = new ServerSocket(12609);

        this.authServerAddr = authServerAddr;

        // Initialize FIle Server to listen on some random port for Replica Server
        // connections
        this.replicaServerSocket = new ServerSocket(12600);

        // Thread Pool to allocate Tasks to
        this.executionPool = Executors.newFixedThreadPool(NTHREADS);

        // Scheduled Executor Service that will be responsible for cleaning up deleted
        // files
        this.fileCleanup = Executors.newSingleThreadScheduledExecutor();

        // Initialize connection to File Database
        // TODO: Get this DB from admin; Remove hardcoding
        this.fileDB = DriverManager.getConnection(url, "root", "85246");
        this.fileDB.setAutoCommit(false);
    }

    public void start() throws IOException, SQLException {
        if (authServerSocket.equals(null))
            throw new NullPointerException("Error. File Server was not initialized!");

        // Start File Cleanup thread
        try {
            fileCleanup.scheduleWithFixedDelay(
                    new DeletionService(this.executionPool, this.authServerAddr, this.fileDB), 0, 20, TimeUnit.MINUTES);
        } catch (Exception e) {
            e.printStackTrace();
            // Try to restart the Cleanup thread
            fileCleanup.scheduleWithFixedDelay(
                    new DeletionService(this.executionPool, this.authServerAddr, this.fileDB), 0, 20, TimeUnit.MINUTES);
        }

        // Begin listening for new Replica Server connections
        executionPool.execute(new ReplicaSocketListener(this.replicaServerSocket, this.executionPool, this.fileDB,
                this.authServerAddr));
        // Begin listening for new Auth Server connections
        while (!executionPool.isShutdown()) {
            try {
                executionPool.execute(new FromAuthHandler(this.authServerSocket.accept(), this.fileDB));
            } catch (IOException e) {
                e.printStackTrace();
                continue;
            }

        }

        this.authServerSocket.close();
        this.fileDB.close();
        this.executionPool.shutdown();
    }

    /**
     * @return Port number the object is listening on
     */
    public int getAuthPort() {
        return this.authServerSocket.getLocalPort();
    }

    /**
     * @return Port number the object is listening on
     */
    public int getReplicaPort() {
        return this.replicaServerSocket.getLocalPort();
    }
}
