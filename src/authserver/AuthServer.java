package authserver;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AuthServer {
    private final static byte NTHREADS = 100;
    private final ExecutorService threadPool;
    private final ServerSocket authServer;

    /**
     * Constructor that automatically starts the Auth Server as a localhost and
     * listens on a random port. The server doesn't begin accepting requests until
     * the start method is called.
     * 
     * @throws IOException
     */
    public AuthServer() throws IOException {
        // Initialize Auth Server to listen on some random port
        authServer = new ServerSocket(0);

        // Thread Pool to allocate Tasks to
        threadPool = Executors.newFixedThreadPool(NTHREADS);

    }

    public void start() throws IOException {
        if (authServer.equals(null))
            throw new NullPointerException("Error. Auth Server was not initialized!");

        // Begin listening for new Socket connections
        while (!threadPool.isShutdown()) {
            try {
                threadPool.execute(new AuthServerHandler(authServer.accept()));
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        this.authServer.close();
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
