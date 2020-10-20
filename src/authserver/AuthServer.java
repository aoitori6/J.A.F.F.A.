package authserver;

import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AuthServer {
    private final static byte NTHREADS = 100;
    private final ExecutorService threadPool;
    private final ServerSocket authServer;

    private final static String url = "jdbc:mysql://localhost:3306/client_database";
    private static Connection clientDB;


    /**
     * Constructor that automatically starts the Auth Server as a localhost and
     * listens on a random port. The server doesn't begin accepting requests until
     * the start method is called.
     * 
     * @throws IOException
     */
    public AuthServer() throws IOException, SQLException {
        // Initialize Auth Server to listen on some random port
        authServer = new ServerSocket(0);

        // Thread Pool to allocate Tasks to
        threadPool = Executors.newFixedThreadPool(NTHREADS);

        AuthServer.clientDB = DriverManager.getConnection(url, "root", "root");

    }

    public void start() {
        if (authServer.equals(null))
            throw new NullPointerException("Error. Auth Server was not initialized!");

        // Begin listening for new Socket connections
        while (!threadPool.isShutdown()) {
            try {
                threadPool.execute(new AuthServerHandler(authServer.accept(), clientDB));
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        threadPool.shutdown();
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
