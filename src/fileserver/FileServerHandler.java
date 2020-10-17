package fileserver;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;

import message.*;
import statuscodes.DownloadStatus;

final public class FileServerHandler implements Runnable {
    private final Socket clientSocket;

    FileServerHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    /**
     * This method contains the central logic of the File Server. It listens for
     * {@code Message} objects from the Client, and handles them as required by
     * looking at the status field.
     */
    public void run() {
        // Expect a Message from the Client
        Message request = MessageHelpers.receiveMessageFrom(this.clientSocket);

        // Central Logic
        // Execute different methods after checking Message status

        switch (request.getRequestKind()) {
            case Download:
                serverDownload((DownloadMessage) request);
                break;
            case Upload:
                break;
            default:
                break;

        }
        try {
            clientSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Takes a DownloadMessage object from the Client and queries the associated
     * File Database if the supplied file code exists. If the code exists, File is
     * sent to the Client, otherwise an error Message is returned.
     * 
     * @param request Dow received from Client
     * 
     *                <p>
     *                Message Specs
     * @expectedInstructionIDs: DOWNLOAD_REQUEST
     * @sentInstructionIDs: DOWNLOAD_START, DOWNLOAD_SUCCESS, DOWNLOAD_FAIL
     * @expectedHeaders: code:code
     * @sentHeaders: fileName:FileName
     */
    private void serverDownload(DownloadMessage request) {
        // TODO: Fetching Actual files from a MySQL DB
        // TODO: Re-checking auth token

        HashMap<String, String> headers = new HashMap<String, String>();

        // For now, simply transfer a standard, 500 MB file;
        if (request.getStatus() == DownloadStatus.DOWNLOAD_REQUEST) {
            // If successfull, send a DOWNLOAD_START message with fileName in Headers
            headers.put("fileName", "test.bin");
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new DownloadMessage(DownloadStatus.DOWNLOAD_START, headers, "File Server", "tempServerKey"));

            // Prep for File transfer
            int buffSize = 1_048_576;
            byte[] writeBuffer = new byte[buffSize];
            BufferedInputStream fileFromDB;
            BufferedOutputStream fileToClient;

            try {
                // Begin connecting to file Server and establish read/write Streams
                fileFromDB = new BufferedInputStream(new FileInputStream("D:\\test.pdf"));
                fileToClient = new BufferedOutputStream(clientSocket.getOutputStream());

                // Temporary var to keep track of read Bytes
                int _temp_c;
                while ((_temp_c = fileFromDB.read(writeBuffer, 0, writeBuffer.length)) != -1) {
                    fileToClient.write(writeBuffer, 0, _temp_c);
                    fileToClient.flush();
                }

                // File successfully downloaded
                fileFromDB.close();
                return;

            } catch (Exception e) {
                e.printStackTrace();
                return;
            } finally {
                writeBuffer = null;
                fileFromDB = null;
                fileToClient = null;
            }
        }

        else {
            headers.put("fileName", null);
            MessageHelpers.sendMessageTo(this.clientSocket,
                    new DownloadMessage(DownloadStatus.DOWNLOAD_FAIL, headers, "File Server", "tempServerKey"));
        }

        headers = null;
    }

}
