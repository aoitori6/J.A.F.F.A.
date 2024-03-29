package server.fileserver.primary;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Random;
import java.util.stream.Stream;

import misc.FileInfo;
import signals.base.Message;
import signals.derived.delete.*;
import signals.derived.download.*;
import signals.derived.filedetails.*;
import signals.derived.upload.*;
import signals.utils.MessageHelpers;

final class FromAuthHandler implements Runnable {
    private final Socket authServer;

    FromAuthHandler(Socket authServer) {
        this.authServer = authServer;
    }

    @Override
    /**
     * This method contains the central logic of the File Server. It continually
     * listens for {@code Message} objects from the Auth Server, and handles them as
     * required by looking at the status field.
     */
    public void run() {
        // Expect a Message from the Client
        Message request = MessageHelpers.receiveMessageFrom(this.authServer);

        // Central Logic
        // Execute different methods after checking Message status

        switch (request.getRequestKind()) {
            case Download:
                serverDownload((DownloadMessage) request);
                break;
            case Upload:
                serverUpload((UploadMessage) request);
                break;
            case Delete:
                deleteFile((DeleteMessage) request);
                break;
            case FileDetails:
                getAllFileData((FileDetailsMessage) request);
                break;
            default:
                break;
        }

        try {
            this.authServer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Takes a DownloadMessage object from the Auth Server and queries the
     * associated File Database if the supplied file code exists. If the code exists
     * and is downloadable, it increases the Current Threads count, and decreases
     * the Downloads_Remaining count (if applicable).
     * 
     * @param request DownloadMessage object from the Auth Server
     * 
     *                <p>
     *                Message Specs
     * @expectedInstructionIDs: DOWNLOAD_REQUEST
     * @sentInstructionIDs: DOWNLOAD_REQUEST_VALID, DOWNLOAD_REQUEST_INVALID
     */
    private void serverDownload(DownloadMessage request) {
        if (request.getStatus() == DownloadStatus.DOWNLOAD_REQUEST) {

            // First check PrimaryFileServer.fileDB if Code exists
            ResultSet queryResp;
            PreparedStatement query;
            boolean canDownload = true;
            boolean toDelete = false;
            try {
                query = PrimaryFileServer.fileDB.prepareStatement(
                        "SELECT Code, Current_Threads, Deletion_Timestamp, Downloads_Remaining FROM file WHERE Code = ?");

                // Querying DB for File details
                query.setString(1, request.getCode());
                queryResp = query.executeQuery();

                // Checking if File actually exists
                if (!queryResp.next()) {
                    MessageHelpers.sendMessageTo(this.authServer,
                            new DownloadMessage(DownloadStatus.DOWNLOAD_REQUEST_INVALID, null, null,
                                    PrimaryFileServer.SERVER_NAME, PrimaryFileServer.SERVER_TOKEN));
                    return;
                }

                // If File exists, check if any download limits have been exceeded
                String downloadsRemaning = queryResp.getString("Downloads_Remaining");
                String timestamp = queryResp.getString("Deletion_Timestamp");
                int currentThreads = queryResp.getInt("Current_Threads");
                query.close();

                // Check if timestamp has been exceeded
                if (timestamp != null) {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    LocalDateTime deletionTimestamp = LocalDateTime.parse(timestamp, formatter);
                    if (LocalDateTime.now(ZoneId.of("UTC")).isAfter(deletionTimestamp)) {
                        toDelete = true;
                        canDownload = false;
                    }
                }

                // Check if download cap has been exceeded
                if (downloadsRemaning != null && !toDelete) {
                    if (Integer.parseInt(downloadsRemaning) == 0) {
                        toDelete = true;
                        canDownload = false;
                    } else if (currentThreads + 1 > Integer.parseInt(downloadsRemaning))
                        canDownload = false;
                }

                String queryString = "";
                // If limits have been exceeded, mark for deletion
                if (toDelete) {
                    queryString = "UPDATE file SET Deletable = TRUE WHERE Code = ?";
                }

                // If downloadable, update necessary fields
                else if (canDownload) {
                    queryString = "UPDATE file SET Current_Threads = Current_Threads + 1 WHERE Code = ?";
                }

                // Executing and comitting changes
                query = PrimaryFileServer.fileDB.prepareStatement(queryString);
                query.setString(1, request.getCode());
                query.executeUpdate();
                query.close();

                PrimaryFileServer.fileDB.commit();

            } catch (Exception e) {
                e.printStackTrace();
                MessageHelpers.sendMessageTo(this.authServer, new DownloadMessage(DownloadStatus.DOWNLOAD_REQUEST_FAIL,
                        null, null, PrimaryFileServer.SERVER_NAME, PrimaryFileServer.SERVER_TOKEN));
                return;
            }

            // File can be downloaded
            if (canDownload) {
                MessageHelpers.sendMessageTo(this.authServer, new DownloadMessage(DownloadStatus.DOWNLOAD_REQUEST_VALID,
                        null, null, PrimaryFileServer.SERVER_NAME, PrimaryFileServer.SERVER_TOKEN));
            }

            // File can't be downloaded
            else {
                MessageHelpers.sendMessageTo(this.authServer,
                        new DownloadMessage(DownloadStatus.DOWNLOAD_REQUEST_INVALID, null, null,
                                PrimaryFileServer.SERVER_NAME, PrimaryFileServer.SERVER_TOKEN));
            }

        } else {
            MessageHelpers.sendMessageTo(this.authServer, new DownloadMessage(DownloadStatus.DOWNLOAD_REQUEST_FAIL,
                    null, null, PrimaryFileServer.SERVER_NAME, PrimaryFileServer.SERVER_TOKEN));
        }
    }

    /**
     * Helper function for serverUpload. First tries to generate a random
     * alphanumeric String, that will both identify the File in the File DB and be
     * its parent folder in the File System.
     * 
     * This method is guaranteed to create a unique File code with respect to the
     * associated DB.
     * 
     * @return {@code true} if a Code was successfully generated and the requisite
     *         directory structure was created, {@code false} otherwise.
     * @throws IOException
     */
    private synchronized Path createFileLocation() throws IOException {
        // Generate a random 5-char alphanumeric String
        // 97 corresponds to 'a' and 122 to 'z'
        String tempCode;
        do {
            tempCode = new Random().ints(97, 122 + 1).limit(5)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();

            // Check DB location in the File System to see if code already exists
        } while (PrimaryFileServer.FILESTORAGEFOLDER_PATH.resolve(tempCode).toFile().exists() == true);

        // Creating new folder named with File Code and return the same
        return Files.createDirectories(PrimaryFileServer.FILESTORAGEFOLDER_PATH.resolve(tempCode));
    }

    /**
     * Takes a UploadMessage object from the Auth Server and begins receiving a File
     * from the same. If the transfer is successful, File is added to the associated
     * File DB and a unique, sharable code is generated and sent back, otherwise an
     * error Message is returned.
     * 
     * @param request UploadMessage received from Auth Server
     * 
     *                <p>
     *                Message Specs
     * @expectedInstructionIDs: UPLOAD_REQUEST
     * @sentInstructionIDs: UPLOAD_START, UPLOAD_FAIL
     * @sentHeaders: filecode:FileCode
     */
    private void serverUpload(UploadMessage request) {
        if (request.getStatus() == UploadStatus.UPLOAD_REQUEST && request.getFileInfo().getName() != null) {
            FileInfo uploadInfo = request.getFileInfo();

            // First generate a Path to the upload folder
            Path fileFolder;
            try {
                fileFolder = createFileLocation();
                uploadInfo.setCode(fileFolder.getFileName().toString());
                // Create the file itself in the Folder
                fileFolder = Files.createFile(fileFolder.resolve(uploadInfo.getName()));
            } catch (IOException e1) {
                e1.printStackTrace();
                MessageHelpers.sendMessageTo(this.authServer, new UploadMessage(UploadStatus.UPLOAD_FAIL, null,
                        PrimaryFileServer.SERVER_NAME, PrimaryFileServer.SERVER_TOKEN, null));
                return;
            }

            // Code generated successfully, signal Client to begin transfer
            MessageHelpers.sendMessageTo(this.authServer, new UploadMessage(UploadStatus.UPLOAD_START, null,
                    PrimaryFileServer.SERVER_NAME, PrimaryFileServer.SERVER_TOKEN, uploadInfo));

            // Prep for File transfer
            int buffSize = 1_048_576;
            byte[] readBuffer = new byte[buffSize];
            BufferedInputStream fileFromAuth = null;

            try (BufferedOutputStream fileToDB = new BufferedOutputStream(
                    new FileOutputStream(fileFolder.toString()));) {
                // Begin connecting to file Server and establish read/write Streams
                fileFromAuth = new BufferedInputStream(this.authServer.getInputStream());

                // Temporary var to keep track of total bytes read
                long _temp_t = 0;
                // Temporary var to keep track of bytes read on each iteration
                int _temp_c = 0;
                while ((_temp_t < uploadInfo.getSize())
                        && ((_temp_c = fileFromAuth.read(readBuffer, 0, Math.min(readBuffer.length,
                                (int) Math.min(uploadInfo.getSize() - _temp_t, Integer.MAX_VALUE)))) != -1)) {
                    fileToDB.write(readBuffer, 0, _temp_c);
                    fileToDB.flush();
                    _temp_t += _temp_c;
                }

            } catch (Exception e) {
                e.printStackTrace();
                MessageHelpers.sendMessageTo(this.authServer, new UploadMessage(UploadStatus.UPLOAD_FAIL, null,
                        PrimaryFileServer.SERVER_NAME, PrimaryFileServer.SERVER_TOKEN, null));
                return;
            } finally {
                readBuffer = null;
                fileFromAuth = null;
            }

            String deletionTimestamp = null;
            if (uploadInfo.getDeletionTimestamp() != null) {
                long totalMinutes = Long.parseLong(uploadInfo.getDeletionTimestamp());
                deletionTimestamp = LocalDateTime.now(ZoneId.of("UTC")).plus(totalMinutes, ChronoUnit.MINUTES)
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            }

            // File successfully transferred
            // If file was successfully uploaded, add an entry to the File DB
            try (PreparedStatement query = PrimaryFileServer.fileDB.prepareStatement(
                    "INSERT INTO file (Code, Uploader, Filename, Downloads_Remaining, Deletion_Timestamp) VALUES(?,?,?,?,?)");) {

                query.setString(1, uploadInfo.getCode());
                query.setString(2, uploadInfo.getUploader());
                query.setString(3, uploadInfo.getName());

                if (uploadInfo.getDownloadsRemaining() != null)
                    query.setInt(4, uploadInfo.getDownloadsRemaining().intValue());
                else
                    query.setNull(4, java.sql.Types.SMALLINT);

                query.setString(5, deletionTimestamp);
                query.executeUpdate();

                PrimaryFileServer.fileDB.commit();
            } catch (Exception e) {
                e.printStackTrace();
                MessageHelpers.sendMessageTo(this.authServer, new UploadMessage(UploadStatus.UPLOAD_FAIL, null,
                        PrimaryFileServer.SERVER_NAME, PrimaryFileServer.SERVER_TOKEN, null));
                return;
            }

            // Notify Auth of Success
            MessageHelpers.sendMessageTo(this.authServer, new UploadMessage(UploadStatus.UPLOAD_SUCCESS, null,
                    PrimaryFileServer.SERVER_NAME, PrimaryFileServer.SERVER_TOKEN, uploadInfo));
        }

        else {
            MessageHelpers.sendMessageTo(this.authServer, new UploadMessage(UploadStatus.UPLOAD_FAIL, null,
                    PrimaryFileServer.SERVER_NAME, PrimaryFileServer.SERVER_TOKEN, null));
        }

    }

    /**
     * Takes a DeleteMesage object from the Auth Server and queries the DB to see if
     * the File exists, and if the requesting Client can delete it. If the query is
     * successful, the File is deleted from both the File DB and the associated File
     * System and the Auth Server is notified. If the query fails, then the Auth
     * Server is notified about the reason for the same.
     * 
     * <p>
     * This method does not cancel any active downloads on the file to be deleted.
     * It only guarantees that the file will be marked as Deletable, and it is up to
     * other methods to respect that field until actual File deletion occurs.
     * 
     * @param request DeleteMessage received from Client
     * 
     *                <p>
     *                Message Specs
     * @expectedInstructionIDs: DELETE_REQUEST
     * @sentInstructionIDs: DELETE_SUCCESS, DELETE_FAIL, DELETE_INVALID
     */

    private void deleteFile(DeleteMessage request) {
        if (request.getStatus() == DeleteStatus.DELETE_REQUEST) {

            // Attempt deletion from the File DB
            PreparedStatement query = null;
            boolean deleted = false;
            try {
                // Preparing Statement
                if (request.checkAdmin()) {
                    query = PrimaryFileServer.fileDB.prepareStatement("DELETE FROM file WHERE code = ?");
                    query.setString(1, request.getCode());
                } else {
                    query = PrimaryFileServer.fileDB
                            .prepareStatement("DELETE FROM file WHERE code = ? AND uploader = ?");
                    query.setString(1, request.getCode());
                    query.setString(2, request.getSender());
                }

                // Executing Query and checking responses
                if (query.executeUpdate() == 1)
                    deleted = true;

                PrimaryFileServer.fileDB.commit();
            } catch (Exception e) {
                try {
                    PrimaryFileServer.fileDB.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            } finally {
                if (query != null)
                    try {
                        query.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
            }

            // If it wasn't deleted from DB
            if (!deleted) {
                MessageHelpers.sendMessageTo(this.authServer, new DeleteMessage(DeleteStatus.DELETE_FAIL, null, null,
                        PrimaryFileServer.SERVER_NAME, PrimaryFileServer.SERVER_TOKEN, request.checkAdmin()));
            }

            // If it was, delete it from the File System too
            Path toBeDeleted = PrimaryFileServer.FILESTORAGEFOLDER_PATH.resolve(request.getCode());
            try (Stream<Path> elements = Files.walk(toBeDeleted)) {
                elements.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("ERROR! Couldn't delete" + toBeDeleted.toString());
            }

            MessageHelpers.sendMessageTo(this.authServer, new DeleteMessage(DeleteStatus.DELETE_SUCCESS, null, null,
                    PrimaryFileServer.SERVER_NAME, PrimaryFileServer.SERVER_TOKEN, request.checkAdmin()));

            // If directory wasn't deleted
            if (Files.exists(toBeDeleted))
                System.err.format("ERROR: File with Code %S was deleted from DB but not from  File System.%n",
                        request.getCode());

        } else {
            MessageHelpers.sendMessageTo(this.authServer, new DeleteMessage(DeleteStatus.DELETE_INVALID, null, null,
                    PrimaryFileServer.SERVER_NAME, PrimaryFileServer.SERVER_TOKEN, request.checkAdmin()));
        }
    }

    /**
     * Takes a FileDetailsMessage object from the Auth Server and queries the
     * associated File DB for a list of files and their details, and sends the same
     * back. A timestamp is sent along with the details, and the results are
     * accurrate to the timestamp.
     * 
     * @param request FileDetailsMessage received from the Auth Server
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
            // If admin, fetch all results
            if (request.checkIfAdmin()) {
                try (Statement query = PrimaryFileServer.fileDB.createStatement();) {
                    ResultSet queryResp = query.executeQuery("SELECT * FROM file WHERE deletable = FALSE");
                    // Parsing Result
                    while (queryResp.next()) {
                        Integer downloadsRemaining = null;
                        if (queryResp.getString("downloads_remaining") != null) {
                            downloadsRemaining = Integer.parseInt(queryResp.getString("downloads_remaining"));
                        }
                        currFileInfo.add(new FileInfo(queryResp.getString("filename"), queryResp.getString("code"),
                                PrimaryFileServer.FILESTORAGEFOLDER_PATH.resolve(queryResp.getString("code")).toFile()
                                        .length(),
                                queryResp.getString("uploader"), downloadsRemaining,
                                queryResp.getString("deletion_timestamp")));
                    }
                    PrimaryFileServer.fileDB.commit();
                } catch (Exception e) {
                    e.printStackTrace();
                    return;
                }
            }

            // Otherwise fetch only User's uploaded files
            else {
                try (PreparedStatement query = PrimaryFileServer.fileDB
                        .prepareStatement("SELECT * FROM file WHERE deletable = FALSE AND Uploader = ?");) {
                    query.setString(1, request.getSender());
                    ResultSet queryResp = query.executeQuery();

                    // Parsing Result
                    while (queryResp.next()) {
                        Integer downloadsRemaining = null;
                        if (queryResp.getString("downloads_remaining") != null) {
                            downloadsRemaining = Integer.parseInt(queryResp.getString("downloads_remaining"));
                        }
                        currFileInfo.add(new FileInfo(queryResp.getString("filename"), queryResp.getString("code"),
                                PrimaryFileServer.FILESTORAGEFOLDER_PATH.resolve(queryResp.getString("code")).toFile()
                                        .length(),
                                queryResp.getString("uploader"), downloadsRemaining,
                                queryResp.getString("deletion_timestamp")));
                    }
                    PrimaryFileServer.fileDB.commit();
                } catch (Exception e) {
                    e.printStackTrace();
                    return;
                }
            }

            // Sending Start message to Auth Server
            HashMap<String, String> headers = new HashMap<String, String>();
            headers.put("count", String.valueOf(currFileInfo.size()));
            headers.put("timestamp",
                    LocalDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            MessageHelpers.sendMessageTo(this.authServer, new FileDetailsMessage(FileDetailsStatus.FILEDETAILS_START,
                    headers, PrimaryFileServer.SERVER_NAME, PrimaryFileServer.SERVER_TOKEN, false));
            headers = null;

            ObjectOutputStream toClient = null;
            try {
                toClient = new ObjectOutputStream(this.authServer.getOutputStream());

                // Beginning transfer of details
                for (FileInfo temp : currFileInfo) {
                    toClient.writeObject(temp);
                }
            } catch (Exception e) {
                e.printStackTrace();
                return;
            } finally {
                currFileInfo = null;
                toClient = null;
            }

        } else {
            MessageHelpers.sendMessageTo(this.authServer, new FileDetailsMessage(FileDetailsStatus.FILEDETAILS_FAIL,
                    null, PrimaryFileServer.SERVER_NAME, PrimaryFileServer.SERVER_TOKEN, false));
        }
    }

}