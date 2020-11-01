package server.fileserver.replica;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import signals.derived.auth.*;

import java.sql.*;

public class ReplicaFileServer {
    /**
     * Instance for Singleton Class
     */
    private static ReplicaFileServer server_instance = null;

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
     * tasks across different Threads in the Replica Server.
     * <p>
     * Has an underlying ThreadPool it reuses to schedule new operations. It can
     * create Threads on demand if needed.
     */
    protected static ExecutorService executionPool;

    /**
     * Hardcoded Server Credentials that may be used to execute certain Admin
     * operations. Equivalent entries MUST exist in the Auth Server's Client DB
     * otherwise certain operations may fail.
     */
    protected final static String SERVER_NAME = "Replica File Server";
    protected final static String SERVER_TOKEN = "mVI3d";

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
     * Address of the Auth Service that the Replica File Server will send requests
     * to verify a user's Auth Token. Currently unnecessary!
     */
    private static InetSocketAddress authServiceListener;

    /**
     * Address of the Primary File Server that the Replica File Server will send
     * requests to for synchronization purposes.
     */
    protected static InetSocketAddress primaryServerAddr = new InetSocketAddress("localhost", 12600);

    /**
     * {@ServerSocket} on which the Replica File Server will listen for connections.
     */
    private final ServerSocket fileServer;

    /**
     * Port on which the Replica File Server will listen for connections
     */
    private static final int REPLICA_LISTENER_PORT = 7689;

    /**
     * Helper function for the class to check a user's credentials. Currently
     * unnecessary.
     * 
     * @param clientName Client's Name
     * @param isAdmin    {@code true} if Client is an Admin, {@code false} otherwise
     * @param authToken  Client's Auth Token
     * @return {@code true} if Client's credentials are valid, {@code false}
     *         otherwise
     */
    protected static boolean checkAuthToken(String clientName, boolean isAdmin, String authToken) {

        try (Socket toService = new Socket(ReplicaFileServer.authServiceListener.getAddress(),
                ReplicaFileServer.authServiceListener.getPort());
                ObjectOutputStream toSocket = new ObjectOutputStream(toService.getOutputStream());
                ObjectInputStream fromSocket = new ObjectInputStream(toService.getInputStream());) {

            toSocket.writeObject(new AuthMessage(AuthStatus.AUTH_CHECK, null, ReplicaFileServer.SERVER_NAME, clientName,
                    authToken, isAdmin));
            toSocket.flush();

            AuthMessage response = (AuthMessage) fromSocket.readObject();
            if (response.getStatus() == AuthStatus.AUTH_VALID)
                return true;
            else
                return false;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private ReplicaFileServer(Connection fileDB, Path storageFolder, InetSocketAddress authServiceListener,
            InetSocketAddress primaryServerAddr) throws IOException, SQLException {
        // MySQL File Database
        ReplicaFileServer.fileDB = fileDB;
        ReplicaFileServer.fileDB.setAutoCommit(false);

        // Seting Auth Service Address to make future connections to
        ReplicaFileServer.authServiceListener = authServiceListener;

        // Setting Storage Folder Path (where received Files are placed)
        ReplicaFileServer.FILESTORAGEFOLDER_PATH = storageFolder;

        // Initialize FIle Server to listen on some random port for connections
        this.fileServer = new ServerSocket(ReplicaFileServer.REPLICA_LISTENER_PORT);

        // Thread Pool to allocate Tasks to
        ReplicaFileServer.executionPool = new ThreadPoolExecutor(ReplicaFileServer.THREADS_PERM,
                ReplicaFileServer.THREADS_MAX, ReplicaFileServer.THREADS_TIMEOUT, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(ReplicaFileServer.THREAD_MAX_INSTRUCTIONS));
    }

    // Singleton instantiation method

    /**
     * Method that attemps to start the Replica File Server as a localhost. The
     * server doesn't begin accepting requests until the start method is called.
     * <p>
     * Returns a singleton instance of the Replica File Server if all services
     * started successfully.
     * 
     * @param fileDB              A Connection to the MySQL Database for storing
     *                            File details
     * @param storageFolder       Path to a folder to store received files
     * @param authServiceListener Address of the Auth Service
     * @param primaryServerAddr   Address of the Primary File Server
     * @return Singleton instance of the Replica File Server
     * @throws IOException  If the server couldn't be initialized
     * @throws SQLException If the server receives an error from the fileDB
     */
    public static ReplicaFileServer getServer(Connection fileDB, Path storageFolder,
            InetSocketAddress authServiceListener, InetSocketAddress primaryServerAddr)
            throws IOException, SQLException {
        if (server_instance == null)
            server_instance = new ReplicaFileServer(fileDB, storageFolder, authServiceListener, primaryServerAddr);

        return server_instance;
    }

    /**
     * Starts the Replica File Server's services and begins listening for requests.
     * 
     * @throws IOException  If any IO error occurs during the lifetime of the Server
     * @throws SQLException If the fileDB {@code Connection} object couldn't be
     *                      closed during termination.
     */
    public void start() throws IOException, SQLException {
        if (fileServer.equals(null))
            throw new NullPointerException("Error. File Server was not initialized!");

        // Begin listening for new Socket connections
        while (!ReplicaFileServer.executionPool.isShutdown()) {
            try {
                ReplicaFileServer.executionPool.execute(new ReplicaFileServerHandler(this.fileServer.accept()));
            } catch (IOException e) {
                e.printStackTrace();
                continue;
            }
        }

        this.fileServer.close();
        ReplicaFileServer.fileDB.close();
        ReplicaFileServer.executionPool.shutdown();
    }

    /**
     * 
     * @return Port number the object is listening on
     */
    public int getServerPort() {
        return this.fileServer.getLocalPort();
    }

    /**
     * Server kill-switch
     */
    public void stopServer() {
        ReplicaFileServer.executionPool.shutdownNow();
    }

}
