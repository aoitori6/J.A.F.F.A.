package fileserver;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.sql.*;

public class FileServer {
    private final static byte NTHREADS = 100;
    private final ExecutorService threadPool;
    private final ScheduledExecutorService fileCleanup;
    private final ServerSocket fileServer;

    private final static String url = "jdbc:mysql://localhost:3306/file_database";
    private static Connection fileDB;

    /**
     * Constructor that automatically starts the File Server as a localhost and
     * listens on a random port. The server doesn't begin accepting requests until
     * the start method is called.
     * 
     * @throws IOException
     * @throws SQLException
     */
    public FileServer() throws IOException, SQLException {
        // Initialize FIle Server to listen on some random port
        fileServer = new ServerSocket(7689);

        // Thread Pool to allocate Tasks to
        threadPool = Executors.newFixedThreadPool(NTHREADS);

        // Scheduled Executor Service that will be responsible for cleaning up deleted
        // files
        fileCleanup = Executors.newSingleThreadScheduledExecutor();

        // Initialize connection to File Database
        // TODO: Get this DB from admin; Remove hardcoding
        FileServer.fileDB = DriverManager.getConnection(url, "root", "85246");
        // FileServer.fileDB.setAutoCommit(false);
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
                threadPool.execute(new FileServerHandler(fileServer.accept(), FileServer.fileDB));
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        this.fileServer.close();
        FileServer.fileDB.close();
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
