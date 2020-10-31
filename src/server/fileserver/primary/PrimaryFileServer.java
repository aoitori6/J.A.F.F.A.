package server.fileserver.primary;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.sql.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

final public class PrimaryFileServer {
    /**
     * Instance for Singleton Class
     */
    private static PrimaryFileServer server_instance = null;

    /**
     * Initial number of Threads the ThreadPool starts out with.
     */
    private final static int THREADS_PERM = 5;

    /**
     * Maximum number of Threads the ThreadPool can have.
     */
    private final static int THREADS_MAX = Integer.MAX_VALUE;

    /**
     * The delay after which the any Thread in excess of {@code THREADS_PERM} will
     * be deleted if it isn't executing any tasks. Value in
     * {@code TimeUnit.MINUTES}.
     */
    private final static int THREADS_TIMEOUT = 15;

    /**
     * Maximum number of requests that can be submitted to the {@code executionPool}
     * simultaneously. Any further submissions will block until the pool is freed
     * up.
     */
    private final static int THREAD_MAX_INSTRUCTIONS = 1000;

    /**
     * {@code ExecutorService} object responsible for executing {@code Runnable}
     * tasks across different Threads in the Primary Server.
     * <p>
     * Has an underlying ThreadPool it reuses to schedule new operations. It can
     * create Threads on demand if needed.
     */
    protected static ExecutorService executionPool;

    /**
     * {@code ScheduledExecutorService} object responsible exclusively for dealing
     * with files tagged with a Deletion Timestamp.
     * <p>
     * It runs on a single Thread and checks every {@code TIMESTAMP_CHECK_FREQUENCY}
     * minutes to see if there are any files whose deletion timestamps have been
     * exceeded, or which are marked as Deletable, and handles their deletion.
     */
    private final ScheduledExecutorService timestampCleanup;

    /**
     * Specifies a delay after which the {@code timestampCleanup} thread will first
     * run. Value in {@code TimeUnit.MINUTES}.
     */

    private final static byte TIMESTAMP_CHECK_INITIALDELAY = 2;
    /**
     * Specifies how frequently the {@code timestampCleanup} thread will run. Value
     * in {@code TimeUnit.MINUTES}.
     */
    private final static byte TIMESTAMP_CHECK_FREQUENCY = 10;

    /**
     * Hardcoded Server Credentials that may be used to execute certain Admin
     * operations. Equivalent entries MUST exist in the Auth Server's Client DB
     * otherwise certain operations may fail.
     */
    protected final static String SERVER_NAME = "Primary File Server";
    protected final static String SERVER_TOKEN = "JbaI6";

    /**
     * Folder that the Primary Server will attempt to save uploaded files in. Files
     * are stored in a nested format as:
     * {@code FILESTORAGEFOLDER_PATH/File_Code/File_Name}
     */
    protected static Path FILESTORAGEFOLDER_PATH;

    /**
     * Connection to a MySQL Database that stores details about the Files. Accessed
     * by threads to store, update and receive File Details. The DB is operated in a
     * non auto-commit mode, to ensure proper synchronization.
     * 
     * <p>
     * The DB Schema is as follows
     * 
     * @Code: VARCHAR(10); NOT NULL, PRIMARY KEY, UNIQUE
     * @Uploader: VARCHAR(30); NOT NULL
     * @Filename: VARCHAR(150); NOT NULL
     * @Downloads_Remaining: BOOLEAN;
     * @Deletion_Timestamp: DATETIME;
     * @Current_Threads: SMALLINT; NOT NULL
     * @Deletable: BOOLEAN;
     */
    protected static Connection fileDB;

    /**
     * Address of the Auth Server that the Primary File Server will connect to as an
     * Admin User for certain operations.
     */
    protected static InetSocketAddress authServerAddr;

    /**
     * {@ServerSocket} on which the Primary File Server will listen for connections
     * from the Auth Server.
     */
    private final ServerSocket authServerSocket;

    /**
     * Port on which the Primary File Server will listen for connections from the
     * Auth Server
     */
    private static final int AUTH_LISTENER_PORT = 12609;

    /**
     * {@ServerSocket} on which the Primary File Server will listen for connections
     * from Replica Servers to resolve file download effects or send uploaded files.
     */
    private final ServerSocket replicaServerSocket;

    /**
     * Port on which the Primary File Server will listen for connections from
     * Replica Servers
     */
    private static final int REPLICA_LISTENER_PORT = 12600;

    private PrimaryFileServer(Connection fileDB, Path storageFolder, InetSocketAddress authServerAddr)
            throws IOException, SQLException {
        // MySQL File Database
        PrimaryFileServer.fileDB = fileDB;
        PrimaryFileServer.fileDB.setAutoCommit(false);

        // Setting Auth Server Address to make future connections to
        PrimaryFileServer.authServerAddr = authServerAddr;

        // Setting Storage Folder Path (where received Files are placed)
        PrimaryFileServer.FILESTORAGEFOLDER_PATH = storageFolder;

        // Initialize FIle Server to listen on specified port for Auth Server
        // connections
        this.authServerSocket = new ServerSocket(PrimaryFileServer.AUTH_LISTENER_PORT);

        // Initialize FIle Server to listen on specified port for Replica Server
        // connections
        this.replicaServerSocket = new ServerSocket(PrimaryFileServer.REPLICA_LISTENER_PORT);

        // Thread Pool to allocate Tasks to
        PrimaryFileServer.executionPool = new ThreadPoolExecutor(PrimaryFileServer.THREADS_PERM,
                PrimaryFileServer.THREADS_MAX, PrimaryFileServer.THREADS_TIMEOUT, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(PrimaryFileServer.THREAD_MAX_INSTRUCTIONS));

        // Scheduled Executor Service that will be responsible for cleaning up deleted
        // files
        this.timestampCleanup = Executors.newSingleThreadScheduledExecutor();
    }

    // Singleton instantiation method

    /**
     * Method that attemps to start the Primary File Server as a localhost. The
     * server doesn't begin accepting requests until the start method is called.
     * <p>
     * Returns a singleton instance of the Primary File Server if all services
     * started successfully.
     * 
     * @param fileDB         A Connection to the MySQL Database for storing
     *                       File details
     * @param storageFolder  Path to a folder to store received files
     * @param authServerAddr Address of the Auth Server
     * @return Singleton instance of the Primary File Server
     * @throws IOException  If the server couldn't be initialized
     * @throws SQLException If the server receives an error from the fileDB
     */
    public static PrimaryFileServer getServer(Connection fileDB, Path storageFolder, InetSocketAddress authServerAddr)
            throws IOException, SQLException {
        if (server_instance == null)
            server_instance = new PrimaryFileServer(fileDB, storageFolder, authServerAddr);

        return server_instance;
    }

    /**
     * Starts the Primary File Server's services and begins listening for requests.
     * 
     * @throws IOException  If any IO error occurs during the lifetime of the Server
     * @throws SQLException If the fileDB {@code Connection} object couldn't be
     *                      closed during termination.
     */
    public void start() throws IOException, SQLException {
        if (authServerSocket.equals(null))
            throw new NullPointerException("Error. File Server was not initialized!");

        // Start File Cleanup thread
        try {
            timestampCleanup.scheduleWithFixedDelay(new DeletionService(),
                    PrimaryFileServer.TIMESTAMP_CHECK_INITIALDELAY, PrimaryFileServer.TIMESTAMP_CHECK_FREQUENCY,
                    TimeUnit.MINUTES);
        } catch (Exception e) {
            e.printStackTrace();
            // Try to restart the Cleanup thread
            timestampCleanup.scheduleWithFixedDelay(new DeletionService(),
                    PrimaryFileServer.TIMESTAMP_CHECK_INITIALDELAY, PrimaryFileServer.TIMESTAMP_CHECK_FREQUENCY,
                    TimeUnit.MINUTES);
        }

        // Begin listening for new Replica Server connections
        executionPool.execute(new ReplicaSocketListener(this.replicaServerSocket));
        // Begin listening for new Auth Server connections
        while (!executionPool.isShutdown()) {
            try {
                executionPool.execute(new FromAuthHandler(this.authServerSocket.accept()));
            } catch (IOException e) {
                e.printStackTrace();
                continue;
            }

        }

        this.authServerSocket.close();
        PrimaryFileServer.fileDB.close();
        PrimaryFileServer.executionPool.shutdown();
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

    /**
     * Server kill-switch
     */
    public void stopServer() {
        PrimaryFileServer.executionPool.shutdownNow();
    }
}