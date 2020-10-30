package fileserver.primary;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;

import misc.FileInfo;
import message.*;
import statuscodes.DownloadStatus;
import statuscodes.FileDetailsStatus;
import statuscodes.SyncUploadStatus;

final class FromReplicaHandler implements Runnable {
    private final Socket replicaServer;
    private final InetSocketAddress authServerAddr;
    private final Connection fileDB;

    private final ExecutorService executionPool;

    private final static Path FILESTORAGEFOLDER_PATH = Paths.get(System.getProperty("user.home"), "sharenow_primarydb");

    FromReplicaHandler(Socket replicaServer, InetSocketAddress authServerAddr, Connection fileDB,
            ExecutorService executionPool) {
        this.replicaServer = replicaServer;
        this.authServerAddr = authServerAddr;
        this.fileDB = fileDB;
        this.executionPool = executionPool;
    }

    @Override
    /**
     * This method contains the central logic of the File Server. It continually
     * listens for {@code Message} objects from the Replica Server, and handles them
     * as required by looking at the status field.
     */
    public void run() {
        // Expect a Message from the Client
        System.err.println("ReplicaHandle: Expecting Message");
        Message request = MessageHelpers.receiveMessageFrom(this.replicaServer);
        System.err.println("ReplicaHandle: Got Message");

        // Central Logic
        // Execute different methods after checking Message status

        switch (request.getRequestKind()) {
            case Download:
                resolveDownloadEffects((DownloadMessage) request);
                break;
            case SyncUpload:
                sendLocalFile((SyncUploadMessage) request);
                break;
            case FileDetails:
                getAllFileData((FileDetailsMessage) request);
                break;
            default:
                break;
        }

        try {
            this.replicaServer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Takes a DownloadMessage object from the Replica Server updates the associated
     * File DB as per the success state of the Message.
     * <p>
     * If the Client's download was successful, it expects DOWNLOAD_SUCCESS and
     * decreases both Current_Threads and Downloads_Remaining fields in the File DB.
     * 
     * <p>
     * If the Client's download wasn't successful, it expects DOWNLOAD_FAIL and
     * decreases the Current_Threads field in the File DB.
     * 
     * @param request DownloadMessage object from the Replica Server
     * 
     *                <p>
     *                Message Specs
     * @expectedInstructionIDs: DOWNLOAD_SUCCESS, DOWNLOAD_FAIL
     * @expectedHeaders: code:Code, client:ClientName
     */
    private void resolveDownloadEffects(DownloadMessage request) {
        if (request.getStatus() == DownloadStatus.DOWNLOAD_SUCCESS) {
            PreparedStatement query = null;
            boolean deletable = false;
            try {
                // Update Downloads Remaining, Current Threads and Deletable if applicable
                query = this.fileDB.prepareStatement(
                        "UPDATE file SET Downloads_Remaining = Downloads_Remaining-1, Current_Threads = Current_Threads-1,"
                                + "Deletable = IF(Downloads_Remaining <= 0, TRUE, Deletable) WHERE Code = ?");
                query.setString(1, request.getCode());
                query.executeUpdate();
                query.close();

                // Check if the File can be deleted
                query = this.fileDB.prepareStatement(
                        "SELECT Code, Deletable FROM file WHERE (Code = ? AND Deletable = TRUE AND Current_Threads = 0)");
                query.setString(1, request.getCode());
                ResultSet queryResp = query.executeQuery();

                // If File can be deleted
                if (queryResp.next())
                    deletable = true;

                this.fileDB.commit();

            } catch (Exception e) {
                e.printStackTrace();
                return;
            } finally {
                try {
                    if (query != null)
                        query.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            // If File is deletable, send a Delete request to Auth Server to ensure deletion
            // is sync'd
            if (deletable)
                this.executionPool.submit(new DeletionToAuth(this.authServerAddr, request.getCode()));

        }

        else if (request.getStatus() == DownloadStatus.DOWNLOAD_FAIL) {
            // Update Current Threads
            try (PreparedStatement query = this.fileDB
                    .prepareStatement("UPDATE file SET Current_Threads = Current_Threads-1 WHERE Code = ?");) {

                query.setString(1, request.getCode());
                query.executeUpdate();
                this.fileDB.commit();

            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
        }

    }

    /**
     * Takes a UploadMessage object from the Replica Server, which contains a File
     * Code in the headers. If valid, it begins sending File to the Replica or
     * returns an error message otherwise.
     * 
     * @param request UploadMessage received from Replica Server
     * 
     *                <p>
     *                Message Specs
     * @expectedInstructionIDs: SYNCUPLOAD_REQUEST
     * @sentInstructionIDs: SYNCUPLOAD_START, SYNCUPLOAD_REQUEST_FAIL,
     */
    private void sendLocalFile(SyncUploadMessage request) {
        if (request.getStatus() == SyncUploadStatus.SYNCUPLOAD_REQUEST) {

            System.err.println("Checking DB for File");
            // First query the File DB
            FileInfo fileInfo;
            try (PreparedStatement query = this.fileDB.prepareStatement("SELECT * FROM file WHERE Code = ?");) {

                query.setString(1, request.getFileCode());
                ResultSet queryResp = query.executeQuery();

                if (!queryResp.next()) {
                    MessageHelpers.sendMessageTo(this.replicaServer, new SyncUploadMessage(
                            SyncUploadStatus.SYNCUPLOAD_FAIL, null, "Primary File Server", "tempToken", null, null));
                    return;
                }

                fileInfo = new FileInfo(queryResp.getString("Filename"), queryResp.getString("Code"),
                        FromReplicaHandler.FILESTORAGEFOLDER_PATH.resolve(queryResp.getString("Code"))
                                .resolve(queryResp.getString("Filename")).toFile().length(),
                        queryResp.getString("Uploader"), queryResp.getInt("Downloads_Remaining"), "Deletion_Timestamp");
                this.fileDB.commit();

            } catch (Exception e) {
                e.printStackTrace();
                MessageHelpers.sendMessageTo(this.replicaServer, new SyncUploadMessage(SyncUploadStatus.SYNCUPLOAD_FAIL,
                        null, "Primary File Server", "tempToken", null, null));
                return;
            }

            System.err.println("Signaling Replica to Start");
            // Signal Replica to prepare for transfer
            MessageHelpers.sendMessageTo(this.replicaServer, new SyncUploadMessage(SyncUploadStatus.SYNCUPLOAD_START,
                    null, "Primary File Server", "tempToken", fileInfo.getCode(), fileInfo));

            // Prep for File transfer
            int buffSize = 1_048_576;
            byte[] readBuffer = new byte[buffSize];
            BufferedOutputStream fileToReplica = null;

            try (BufferedInputStream fileFromDB = new BufferedInputStream(
                    new FileInputStream(FromReplicaHandler.FILESTORAGEFOLDER_PATH.resolve(fileInfo.getCode())
                            .resolve(fileInfo.getName()).toString()));) {

                // Begin connecting to file Server and establish read/write Streams
                fileToReplica = new BufferedOutputStream(this.replicaServer.getOutputStream());

                // Temporary var to keep track of total bytes read
                long _temp_t = 0;
                // Temporary var to keep track of bytes read on each iteration
                int _temp_c = 0;
                while ((_temp_t < fileInfo.getSize()) && ((_temp_c = fileFromDB.read(readBuffer, 0,
                        Math.min(readBuffer.length, (int) fileInfo.getSize()))) != -1)) {
                    fileToReplica.write(readBuffer, 0, _temp_c);
                    fileToReplica.flush();
                    _temp_t += _temp_c;
                }

            } catch (Exception e) {
                e.printStackTrace();
                MessageHelpers.sendMessageTo(this.replicaServer, new SyncUploadMessage(SyncUploadStatus.SYNCUPLOAD_FAIL,
                        null, "File Server", "tempServerKey", null, fileInfo));
                return;
            } finally {
                readBuffer = null;
                if (fileToReplica != null)
                    fileToReplica = null;
            }

            // File transfer done
            System.err.println("File Transfer Done");
            MessageHelpers.sendMessageTo(this.replicaServer, new SyncUploadMessage(SyncUploadStatus.SYNCUPLOAD_SUCCESS,
                    null, "File Server", "tempServerKey", null, null));
        } else {
            MessageHelpers.sendMessageTo(this.replicaServer, new SyncUploadMessage(SyncUploadStatus.SYNCUPLOAD_FAIL,
                    null, "Primary File Server", "tempToken", null, null));
        }
    }

    /**
     * Takes a FileDetailsMessage object from the Replica Server and queries the
     * associated File DB for a list of files and their details, and sends the same
     * back. A timestamp is sent along with the details, and the results are
     * accurrate to the timestamp.
     * 
     * @param request FileDetailsMessage received from the Replica Server
     * 
     *                <p>
     *                Message Specs
     * @expectedInstructionIDs: FILEDETAILS_REQUEST
     * @sentInstructionIDs: FILEDETAILS_SUCCESS, FILEDETAILS_FAIL
     * @sentHeaders: count:FileCount, timestamp:ServerTimestamp (at which time data
     *               was fetched)
     */
    private void getAllFileData(FileDetailsMessage request) {
        if (request.getStatus() == FileDetailsStatus.FILEDETAILS_REQUEST) {
            ArrayList<FileInfo> currFileInfo = new ArrayList<FileInfo>(0);

            // Querying associated File DB
            try (Statement query = fileDB.createStatement();) {
                ResultSet queryResp = query.executeQuery("SELECT * FROM files WHERE deletable = FALSE");
                // Parsing Result
                while (queryResp.next()) {
                    currFileInfo.add(new FileInfo(queryResp.getString("filename"), queryResp.getString("code"),
                            FILESTORAGEFOLDER_PATH.resolve(queryResp.getString("code")).toFile().length(),
                            queryResp.getString("uploader"),
                            Integer.parseInt(queryResp.getString("downloads_remaining")),
                            queryResp.getString("deletion_timestamp")));
                }
                this.fileDB.commit();
            } catch (SQLException e) {
                e.printStackTrace();
                return;
            }

            try (ObjectOutputStream toClient = new ObjectOutputStream(this.replicaServer.getOutputStream());) {
                // Sending Start message to Replica Server
                HashMap<String, String> headers = new HashMap<String, String>();
                headers.put("count", String.valueOf(currFileInfo.size()));
                headers.put("timestamp", new Date().toString());
                MessageHelpers.sendMessageTo(this.replicaServer, new FileDetailsMessage(
                        FileDetailsStatus.FILEDETAILS_START, headers, "File Server", "tempAuthToken"));
                headers = null;

                // Beginning transfer of details
                for (FileInfo temp : currFileInfo)
                    toClient.writeObject(temp);

            } catch (Exception e) {
                e.printStackTrace();
                return;
            } finally {
                currFileInfo = null;
            }

        } else {
            MessageHelpers.sendMessageTo(this.replicaServer,
                    new FileDetailsMessage(FileDetailsStatus.FILEDETAILS_FAIL, null, "File Server", "tempServerKey"));
        }
    }

}
