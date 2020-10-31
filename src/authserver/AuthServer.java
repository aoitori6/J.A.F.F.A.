package authserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AuthServer {
    private final static byte NTHREADS = 12;
    private final ExecutorService clientThreadPool;
    private final ServerSocket authServer;
    private final ServerSocket authService;

    private final static String url = "jdbc:mysql://localhost:3306/client_database";
    private Connection clientDB;
    private InetSocketAddress primaryServerAddress = new InetSocketAddress("localhost", 12609);

    private CopyOnWriteArrayList<InetSocketAddress> replicaAddrs = new CopyOnWriteArrayList<InetSocketAddress>();

    /**
     * Constructor that automatically starts the Auth Server as a localhost and
     * listens on a random port. The server doesn't begin accepting requests until
     * the start method is called.
     * 
     * @param replicaAddrs List of Replica Servers the Auth Server will attempt to
     *                     send synchronization requests to.
     * @throws IOException  If Server couldn't be initialized
     * @throws SQLException If Server couldn't establish a connection to the MySQL
     *                      DB
     */
    public AuthServer(ArrayList<InetSocketAddress> replicaAddrs) throws IOException, SQLException {
        // Initialize Auth Server to listen on some random port
        authServer = new ServerSocket(9000);

        // Initalize Auth Service to listen on some random port
        authService = new ServerSocket(10000);

        // Thread Pool to allocate Tasks to
        clientThreadPool = Executors.newFixedThreadPool(NTHREADS);

        // Client Database to authenticate against
        this.clientDB = DriverManager.getConnection(url, "root", "85246");
        this.clientDB.setAutoCommit(false);

        for (InetSocketAddress replicaAddr : replicaAddrs) {
            this.replicaAddrs.add(replicaAddr);
        }
        System.out.print(this.replicaAddrs.size());
    }

    public void start() throws SQLException, IOException, InterruptedException {
        if (authServer.equals(null))
            throw new NullPointerException("Error. Auth Server was not initialized!");

        // Begin listening for new Auth Requests
        clientThreadPool.execute(new AuthServiceListener(this.authService, this.clientThreadPool, this.clientDB));

        // Begin listening for new Socket connections
        while (!clientThreadPool.isShutdown()) {
            try {
                clientThreadPool.execute(new AuthServerHandler(authServer.accept(), this.clientDB,
                        this.primaryServerAddress, this.replicaAddrs, this.clientThreadPool));
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        this.clientThreadPool.shutdown();
        this.clientThreadPool.awaitTermination(15, TimeUnit.MINUTES);

        // Closing Client DB Connection
        this.clientDB.close();

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
        this.clientThreadPool.shutdown();
    }
}
