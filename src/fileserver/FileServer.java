package fileserver;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileServer {
    private final static byte NTHREADS = 100;
    private final ExecutorService threadPool;
    private final ServerSocket fileServer;

    /**
     * Constructor that automatically starts the File Server as a localhost and
     * listens on a random port. The server doesn't begin accepting requests until
     * the start method is called.
     * 
     * @throws IOException
     */
    public FileServer() throws IOException {
        // Initialize FIle Server to listen on some random port
        fileServer = new ServerSocket(7689);

        // Thread Pool to allocate Tasks to
        threadPool = Executors.newFixedThreadPool(NTHREADS);

    }

    public void start() throws IOException {
        if (fileServer.equals(null))
            throw new NullPointerException("Error. File Server was not initialized!");

        // Begin listening for new Socket connections
        while (!threadPool.isShutdown()) {
            try {
                threadPool.execute(new FileServerHandler(fileServer.accept()));
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        this.fileServer.close();
        threadPool.shutdown();
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
