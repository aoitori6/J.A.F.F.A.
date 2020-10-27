package fileserver.replica;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;

class DeletionService implements Runnable {
    private final static String url = "jdbc:mysql://localhost:3306/file_database";
    private final static Path dbLocation = Paths.get(System.getProperty("user.home"), "sharenowdb");

    @Override
    public void run() {
        // This method is responsible for cleaning up dead Files in the File System, i.e
        // Files whose entries have been deleted from the associated File DB but still
        // persist on the local File System.

        // List of Files to be deleted
        ArrayList<File> fileCodes = new ArrayList<File>();

        // Establish connection to File DB and query it for deletable files
        try (Connection fileDB = DriverManager.getConnection(url, "root", "85246");
                Statement query = fileDB.createStatement();
                ResultSet queryResp = query.executeQuery("SELECT * FROM file WHERE deletable = TRUE AND Current_Threads = 0");) {

            while (queryResp.next())
                fileCodes.add(dbLocation.resolve(queryResp.getString("code")).toFile());

        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        // Now delete entries from the File DB
        try (Connection fileDB = DriverManager.getConnection(url, "root", "85246");
                Statement query = fileDB.createStatement();) {
            query.executeUpdate("DELETE FROM file WHERE deletable = TRUE");
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        // Recursively deleting all the Files in the folder
        for (File toBeDeleted : fileCodes) {
            if (toBeDeleted.exists()) {
                try {
                    Files.walk(toBeDeleted.toPath()).sorted(Comparator.reverseOrder()).map(Path::toFile)
                            .forEach(File::delete);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("ERROR! Couldn't delete" + toBeDeleted.getPath());
                    continue;
                }
            }
        }

    }

}
