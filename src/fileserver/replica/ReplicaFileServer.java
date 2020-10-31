package fileserver.replica;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.sql.*;

import message.AuthMessage;
import statuscodes.AuthStatus;

public class ReplicaFileServer {
    private final static byte NTHREADS = 100;
    private final ExecutorService threadPool;

    private final ServerSocket fileServer;
    private final InetSocketAddress authServiceListener = new InetSocketAddress("localhost", 10000);

    private final InetSocketAddress primaryServerAddr = new InetSocketAddress("localhost", 12600);

    private final static String url = "jdbc:mysql://localhost:3306/file_database";
    private final Connection fileDB;

    protected static boolean checkAuthToken(InetSocketAddress authServiceListener, String clientName, boolean isAdmin,
            String authToken) {

        try (Socket toService = new Socket(authServiceListener.getAddress(), authServiceListener.getPort());
                ObjectOutputStream toSocket = new ObjectOutputStream(toService.getOutputStream());
                ObjectInputStream fromSocket = new ObjectInputStream(toService.getInputStream());) {

            toSocket.writeObject(
                    new AuthMessage(AuthStatus.AUTH_CHECK, null, "Replica Server", clientName, authToken, isAdmin));
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

    }

    public void start() throws IOException, SQLException {
        if (fileServer.equals(null))
            throw new NullPointerException("Error. File Server was not initialized!");

        // Begin listening for new Socket connections
        while (!threadPool.isShutdown()) {
            try {
                threadPool.execute(new ReplicaFileServerHandler(this.fileServer.accept(), this.fileDB,
                        this.primaryServerAddr, this.authServiceListener));
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        this.fileServer.close();
        this.fileDB.close();
        this.threadPool.shutdown();
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
