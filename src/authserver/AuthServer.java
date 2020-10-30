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
import java.util.concurrent.TimeUnit;

public class AuthServer {
    private final static byte NTHREADS = 12;
    private final ExecutorService clientThreadPool;
    private final ServerSocket authServer;

    private final static String url = "jdbc:mysql://localhost:3306/client_database";
    private Connection clientDB;
    private InetSocketAddress primaryServerAddress = new InetSocketAddress("localhost", 12609);

    private ArrayList<InetSocketAddress> authRequesterAddrs = new ArrayList<InetSocketAddress>(1);
    private HashMap<InetSocketAddress, Socket> authRequesterSockts = new HashMap<InetSocketAddress, Socket>(1);

    private ArrayList<InetSocketAddress> replicaAddrs = new ArrayList<InetSocketAddress>(1);

    /**
     * Constructor that automatically starts the Auth Server as a localhost and
     * listens on a random port. The server doesn't begin accepting requests until
     * the start method is called.
     * 
     * @param authRequesterAddrs List of Addresses the Auth Server will monitor and
     *                           maintain Sockets to to provide Authentication
     *                           Services.
     * @param replicaAddrs       List of Replica Servers the Auth Server will
     *                           attempt to send synchronization requests to.
     * @throws IOException  If Server couldn't be initialized
     * @throws SQLException If Server couldn't establish a connection to the MySQL
     *                      DB
     */
    public AuthServer(ArrayList<InetSocketAddress> authRequesterAddrs, ArrayList<InetSocketAddress> replicaAddrs)
            throws IOException, SQLException {
        // Initialize Auth Server to listen on some random port
        authServer = new ServerSocket(9000);

        // Thread Pool to allocate Tasks to
        clientThreadPool = Executors.newFixedThreadPool(NTHREADS);

        // Client Database to authenticate against
        this.clientDB = DriverManager.getConnection(url, "root", "85246");
        this.clientDB.setAutoCommit(false);

        // Attempt to establish connection to Auth Requesters to test validity
        Socket tempSock;
        for (InetSocketAddress authRequester : authRequesterAddrs) {
            try {
                tempSock = new Socket(authRequester.getHostName(), authRequester.getPort());
                this.authRequesterSockts.put(authRequester, tempSock);
                this.authRequesterAddrs.add(authRequester);
            } catch (Exception e) {
                e.printStackTrace();
                this.authRequesterAddrs.remove(authRequester);
                System.err.format("ERROR: Couldn't connect to Auth Requester %s! Ignoring.%n",
                        authRequester.toString());
                continue;
            }
        }

        for (InetSocketAddress replicaAddr : replicaAddrs) {
            this.replicaAddrs.add(replicaAddr);
        }
        // Attempt to establish connection to Replica Servers
        /*
         * for (InetSocketAddress replicaAddr : replicaAddrs) { try { tempSock = new
         * Socket(replicaAddr.getHostName(), replicaAddr.getPort());
         * this.replicaAddrs.add(replicaAddr); } catch (Exception e) {
         * e.printStackTrace(); this.replicaAddrs.remove(replicaAddr); System.err.
         * format("ERROR: Couldn't connect to Replica File Server %s! Ignoring.%n",
         * replicaAddr.toString()); continue; } }
         */
    }

    public void start() throws SQLException, IOException, InterruptedException {
        if (authServer.equals(null))
            throw new NullPointerException("Error. Auth Server was not initialized!");

        // Starting Auth Service listeners
        for (InetSocketAddress authRequester : this.authRequesterAddrs) {
            clientThreadPool
                    .execute(new AuthServerVerifyHandle(this.authRequesterSockts.get(authRequester), this.clientDB));
            System.err.format("INFO: Started Thread listening to Auth Requester %s.%n", authRequester.toString());
        }

        // Begin listening for new Socket connections
        while (!clientThreadPool.isShutdown()) {
            try {
                clientThreadPool.execute(new AuthServerHandler(authServer.accept(), this.clientDB,
                        this.primaryServerAddress, this.replicaAddrs));
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        this.clientThreadPool.shutdown();
        this.clientThreadPool.awaitTermination(15, TimeUnit.MINUTES);

        // Closing Client DB Connection
        this.clientDB.close();

        // Closing Auth Service Sockets
        for (InetSocketAddress authRequester : this.authRequesterAddrs) {
            this.authRequesterSockts.get(authRequester).close();
            System.err.format("INFO: Stopped Socket listening to Auth Requester %s.%n", authRequester.toString());
        }

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
