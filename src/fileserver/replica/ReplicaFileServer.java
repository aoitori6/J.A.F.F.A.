package fileserver.replica;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.sql.*;

import message.AuthMessage;
import message.MessageHelpers;
import statuscodes.AuthStatus;

public class ReplicaFileServer {
    private final static byte NTHREADS = 100;
    private final ExecutorService threadPool;
    private final ScheduledExecutorService fileCleanup;
    private final ServerSocket fileServer;
    private final ServerSocket authServerListener;
    private static Socket authServer;
    private Socket primaryFileServerSocket;

    private final static String url = "jdbc:mysql://localhost:3306/file_database";
    private static Connection fileDB;

    protected static synchronized boolean checkAuthToken(String clientName, String authToken) {
        AuthMessage response;
        try {
            if (!MessageHelpers.sendMessageTo(ReplicaFileServer.authServer,
                    new AuthMessage(AuthStatus.AUTH_CHECK, null, "File Server", clientName, authToken, false)))
                return false;

            response = (AuthMessage) MessageHelpers.receiveMessageFrom(ReplicaFileServer.authServer);
            if (response.getStatus() == AuthStatus.AUTH_VALID)
                return true;
            else
                return false;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            response = null;
        }
    }

    /**
     * Constructor that automatically starts the File Server as a localhost and
     * listens on a random port. The server doesn't begin accepting requests until
     * the start method is called.
     * 
     * @throws IOException
     * @throws SQLException
     */
    public ReplicaFileServer() throws IOException, SQLException {
        // Initialize FIle Server to listen on some random port
        this.fileServer = new ServerSocket(7689);

        // Thread Pool to allocate Tasks to
        this.threadPool = Executors.newFixedThreadPool(NTHREADS);

        // Scheduled Executor Service that will be responsible for cleaning up deleted
        // files
        this.fileCleanup = Executors.newSingleThreadScheduledExecutor();

        // Initialize connection to File Database
        // TODO: Get this DB from admin; Remove hardcoding
        ReplicaFileServer.fileDB = DriverManager.getConnection(url, "root", "85246");
        // FileServer.fileDB.setAutoCommit(false);

        // Try to connect to the Primary File Server
        try {
            this.primaryFileServerSocket = new Socket("localhost", 12600);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Initialize connection point to Auth Server
        this.authServerListener = new ServerSocket(9696);
        ReplicaFileServer.authServer = this.authServerListener.accept();
    }

    public void start() throws IOException, SQLException {
        if (fileServer.equals(null))
            throw new NullPointerException("Error. File Server was not initialized!");

        // Start File Cleanup thread
        try {
            fileCleanup.scheduleWithFixedDelay(new DeletionService(), 0, 20, TimeUnit.MINUTES);
        } catch (Exception e) {
            e.printStackTrace();
            // Try to restart the Cleanup thread
            fileCleanup.scheduleWithFixedDelay(new DeletionService(), 0, 20, TimeUnit.MINUTES);
        }

        // Begin listening for new Socket connections
        while (!threadPool.isShutdown()) {
            try {
                threadPool.execute(new ReplicaFileServerHandler(this.fileServer.accept(), ReplicaFileServer.fileDB,
                        this.primaryFileServerSocket, ReplicaFileServer.authServer));
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        this.fileServer.close();
        ReplicaFileServer.fileDB.close();
        this.threadPool.shutdown();
        ReplicaFileServer.authServer.close();
        this.authServerListener.close();
    }

    /**
     * Gets the port number associated with the fileServer object
     * 
     * @return Port number the object is listening on
     */
    public int getServerPort() {
        return this.fileServer.getLocalPort();
    }

}
