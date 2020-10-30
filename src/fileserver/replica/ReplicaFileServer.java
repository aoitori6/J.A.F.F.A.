package fileserver.replica;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.sql.*;

import message.AuthMessage;
import message.MessageHelpers;
import statuscodes.AuthStatus;

public class ReplicaFileServer {
    private final static byte NTHREADS = 100;
    private final ExecutorService threadPool;
    private final ServerSocket fileServer;
    private final ServerSocket authServiceListener;
    private static Socket authService;
    private final InetSocketAddress primaryServerAddr = new InetSocketAddress("localhost", 12600);

    private final static String url = "jdbc:mysql://localhost:3306/file_database";
    private final Connection fileDB;

    protected static synchronized boolean checkAuthToken(String clientName, String authToken) {
        AuthMessage response;
        try {
            if (!MessageHelpers.sendMessageTo(ReplicaFileServer.authService,
                    new AuthMessage(AuthStatus.AUTH_CHECK, null, "File Server", clientName, authToken, false)))
                return false;

            response = (AuthMessage) MessageHelpers.receiveMessageFrom(ReplicaFileServer.authService);
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

        // Initialize connection to File Database
        // TODO: Get this DB from admin; Remove hardcoding
        this.fileDB = DriverManager.getConnection(url, "root", "85246");
        this.fileDB.setAutoCommit(false);

        // Initialize connection point to Auth Server
        this.authServiceListener = new ServerSocket(9696);
        ReplicaFileServer.authService = this.authServiceListener.accept();
    }

    public void start() throws IOException, SQLException {
        if (fileServer.equals(null))
            throw new NullPointerException("Error. File Server was not initialized!");

        // Begin listening for new Socket connections
        while (!threadPool.isShutdown()) {
            try {
                threadPool.execute(new ReplicaFileServerHandler(this.fileServer.accept(), this.fileDB,
                        this.primaryServerAddr, ReplicaFileServer.authService));
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        this.fileServer.close();
        this.fileDB.close();
        this.threadPool.shutdown();
        ReplicaFileServer.authService.close();
        this.authServiceListener.close();
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
