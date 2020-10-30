package fileserver.primary;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

class DeletionService implements Runnable {
    private final ExecutorService executionPool;
    private final Connection fileDB;
    private InetSocketAddress authServerAddr;

    private final static Path FILESTORAGEFOLDER_PATH = Paths.get(System.getProperty("user.home"), "sharenow_primarydb");

    DeletionService(ExecutorService executionPool, InetSocketAddress authServerAddr, Connection fileDB) {
        this.executionPool = executionPool;
        this.authServerAddr = authServerAddr;
        this.fileDB = fileDB;
    }

    @Override
    public void run() {
        // This service is responsible for deleting files with Deletion Timestamps

        // List of Files to be deleted
        ArrayList<Path> fileCodes = new ArrayList<Path>();

        // Querying DB for deletable files
        PreparedStatement query = null;
        try {
            query = this.fileDB
                    .prepareStatement("SELECT Code FROM file WHERE (Deletion_Timestamp <= ? OR Deletable = TRUE)");
            String currTime = LocalDateTime.now(ZoneId.of("UTC"))
                    .format(DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"));
            query.setString(1, currTime);
            ResultSet queryResp = query.executeQuery();

            // Deleting entries from DB
            while (queryResp.next())
                fileCodes.add(FILESTORAGEFOLDER_PATH.resolve(queryResp.getString("Code")));
            query.close();
            this.fileDB.commit();

        } catch (Exception e) {
            try {
                this.fileDB.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            if (query != null)
                try {
                    query.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
        }

        // Recursively deleting all the Files in the folder and sending Deletion
        // Requests to Auth for synchronization
        for (Path toBeDeleted : fileCodes) {
            this.executionPool.submit(new DeletionToAuth(this.authServerAddr, toBeDeleted.getFileName().toString()));
        }

    }

}
