package server.authserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

final public class AuthServer {
    /**
     * Instance for Singleton Class
     */
    private static AuthServer server_instance = null;

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
     * Hardcoded Server Credentials. Equivalent entries MUST exist in the Auth
     * Server's Client DB otherwise certain operations may fail.
     */
    protected final static String SERVER_NAME = "Auth Server";
    protected final static String SERVER_TOKEN = "kQFqW";

    /**
     * Connection to a MySQL Database that stores details about the Users. Accessed
     * by threads to store, update and receive Client Details. The DB is operated in
     * a non auto-commit mode, to ensure proper synchronization.
     * 
     * <p>
     * The DB Schema is as follows
     * 
     * @Username: VARCHAR(30); NOT NULL, PRIMARY KEY, UNIQUE
     * @Password: CHAR(64); NOT NULL
     * @Login_Status: ENUM('ONLINE','OFFLINE'); NOT NULL, DEFAULT('OFFLINE')
     * @Admin_Status: BOOLEAN; NOT NULL
     * @Auth_Code: VARCHAR(5);
     * 
     */
    protected static Connection clientDB;

    /**
     * Address of the Primary File Server that the Auth Server will connect to to
     * forward certain requests.
     */
    protected static InetSocketAddress primaryServerAddress;

    /**
     * {@ServerSocket} on which the Auth Server will listen for connections from
     * Clients to interact with the File Network.
     */
    private final ServerSocket authServer;

    /**
     * Port on which the Primary File Server will listen for connections from
     * Clients
     */
    private static final int CLIENT_LISTENER_PORT = 14000;

    /**
     * {@ServerSocket} on which the Auth Server will operate an Auth Service
     * function to verify Client details for other servers as requested.
     */
    private final ServerSocket authService;

    /**
     * Port on which the Auth Service will listen for connections from other Servers
     */
    private static final int AUTHSERVICE_LISTENER_PORT = 10000;

    /**
     * List of Replica Server addresses that the Auth Server will keep synchronized
     * and direct Clients to for appropriate operations.
     */
    protected static CopyOnWriteArrayList<InetSocketAddress> replicaAddrs = new CopyOnWriteArrayList<InetSocketAddress>();

    /**
     * HashMap for situations where the Auth Server must connection to one address,
     * but must direct a Client to another address for the same Replica File Server.
     * 
     * <p>
     * For each Replica File Server, it contains entries where the Key is an
     * {@code InetSocketAddress} that the Auth Server will attempt to connect to,
     * and the Key-value is an {@code InetSocketAddress} that the Auth Server will
     * direct Clients to connect to.
     */
    protected static HashMap<InetSocketAddress, InetSocketAddress> replicaAddrsForClient = new HashMap<InetSocketAddress, InetSocketAddress>(
            1);

    private AuthServer(Connection clientDB, InetSocketAddress primaryServerAddress,
            HashMap<InetSocketAddress, InetSocketAddress> replicaAddrs) throws IOException, SQLException {
        // MySQL Client Database
        AuthServer.clientDB = clientDB;
        AuthServer.clientDB.setAutoCommit(false);

        // Setting Primary Server Address to make future connections to
        AuthServer.primaryServerAddress = primaryServerAddress;

        // Initialize Auth Server to listen on some random port
        authServer = new ServerSocket(AuthServer.CLIENT_LISTENER_PORT);

        // Initalize Auth Service to listen on some random port
        authService = new ServerSocket(AuthServer.AUTHSERVICE_LISTENER_PORT);

        // Thread Pool to allocate Tasks to
        AuthServer.executionPool = new ThreadPoolExecutor(AuthServer.THREADS_PERM, AuthServer.THREADS_MAX,
                AuthServer.THREADS_TIMEOUT, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(AuthServer.THREAD_MAX_INSTRUCTIONS));

        replicaAddrs.forEach((auth, client) -> {
            AuthServer.replicaAddrsForClient.put(auth, client);
            AuthServer.replicaAddrs.add(auth);
        });

    }

    // Singleton instantiation method

    /**
     * Method that attemps to start the Auth Server as a localhost. The server
     * doesn't begin accepting requests until the start method is called.
     * <p>
     * Returns a singleton instance of the Auth Server if all services started
     * successfully.
     * 
     * @param clientDB             A Connection to the MySQL Database for storing
     *                             File details
     * @param primaryServerAddress Address of the Primary File Server
     * @param replicaAddrs         HashMap of Replica Servers the Auth Server will
     *                             attempt to send synchronization requests to and
     *                             will direct the Client to.
     *                             <p>
     *                             The address the Auth will connect to must be the
     *                             Key, and the Key-value must be the address the
     *                             Client will connect to.
     * @return Singleton instance of the Auth Server
     * @throws IOException  If the server couldn't be initialized
     * @throws SQLException If the server receives an error from the clientDB
     */
    public static AuthServer getServer(Connection clientDB, InetSocketAddress primaryServerAddress,
            HashMap<InetSocketAddress, InetSocketAddress> replicaAddrs) throws IOException, SQLException {
        if (server_instance == null)
            server_instance = new AuthServer(clientDB, primaryServerAddress, replicaAddrs);

        return server_instance;
    }

    public void start() throws SQLException, IOException, InterruptedException {
        if (authServer.equals(null))
            throw new NullPointerException("Error. Auth Server was not initialized!");

        // Begin listening for new Auth Requests
        AuthServer.executionPool.execute(new AuthServiceListener(this.authService));

        // Begin listening for new Socket connections
        while (!AuthServer.executionPool.isShutdown()) {
            try {
                AuthServer.executionPool.execute(new AuthServerHandler(authServer.accept()));
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        AuthServer.executionPool.shutdown();
        AuthServer.executionPool.awaitTermination(15, TimeUnit.MINUTES);

        // Closing Client DB Connection
        AuthServer.clientDB.close();

    }

    /**
     * Gets the port number associated with the AuthServer object
     * 
     * @return Port number the object is listening on
     */
    public int getServerPort() {
        return this.authServer.getLocalPort();
    }

    /**
     * Gets the port number associated with the Auth Service
     * 
     * @return Port number the Auth Service is listening on
     */
    public int getAuthServicePort() {
        return this.authService.getLocalPort();
    }

    public void shutDown() {
        AuthServer.executionPool.shutdown();
    }
}
