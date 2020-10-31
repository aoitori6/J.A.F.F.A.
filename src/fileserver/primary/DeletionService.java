package fileserver.primary;

import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

final class DeletionService implements Runnable {

    @Override
    /**
     * Service responsible for deleting files with Deletion Timestamps or with
     * Deletable set to TRUE
     */
    public void run() {
        // List of Files to be deleted
        ArrayList<Path> fileCodes = new ArrayList<Path>();

        // Querying DB for deletable files
        PreparedStatement query = null;
        try {
            query = PrimaryFileServer.fileDB
                    .prepareStatement("SELECT Code FROM file WHERE (Deletion_Timestamp <= ? OR Deletable = TRUE)");
            String currTime = LocalDateTime.now(ZoneId.of("UTC"))
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            query.setString(1, currTime);
            ResultSet queryResp = query.executeQuery();

            // Deleting entries from DB
            while (queryResp.next())
                fileCodes.add(PrimaryFileServer.FILESTORAGEFOLDER_PATH.resolve(queryResp.getString("Code")));
            query.close();
            PrimaryFileServer.fileDB.commit();

        } catch (Exception e) {
            try {
                PrimaryFileServer.fileDB.rollback();
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
            PrimaryFileServer.executionPool.submit(new DeletionToAuth(toBeDeleted.getFileName().toString()));
        }

    }

}
