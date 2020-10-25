package adminclient;

import java.util.ArrayList;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.HashMap;

import client.Client;
import message.*;
import statuscodes.*;

public class Admin extends Client {

    public Admin(String address, int port) {
        super(address, port);
    }

    /**
     * @return A list of available File Information from the File DB
     * 
     * 
     *         <p>
     *         Message Specs
     * @sentInstructionIDs: FILEDETAILS_REQUEST
     * @expectedInstructionIDs: FILEDETAILS_SUCCESS, FILEDETAILS_FAIL
     * @expectedHeaders: count:FileCount, timestamp:ServerTimestamp (at which time
     *                   data was feteched)
     */
    public ArrayList<FileInfo> getAllFileData() {
        // Send a LocateServerStatus to the Central Server
        // with isUploadRequest set to true

        // Expect addr:ServerAddress, port:ServerPort if Successful
        HashMap<String, String> fileServerAddress;
        try {
            fileServerAddress = fetchServerAddress(
                    new LocateServerMessage(LocateServerStatus.GET_SERVER, null, this.name, this.authToken, true));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        // If valid Address returned, attempt to connect to FileServer
        try {
            this.fileSocket = new Socket(fileServerAddress.get("addr"),
                    Integer.parseInt(fileServerAddress.get("port")));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        // Send a FileDetailsRequest to the Central File Server
        // Expect FILEDETAILS_START if file exists and all is successful
        if (!MessageHelpers.sendMessageTo(fileSocket,
                new FileDetailsMessage(FileDetailsStatus.FILEDETAILS_REQUEST, null, name, this.authToken)))
            return null;

        // Parse Response
        Message response = MessageHelpers.receiveMessageFrom(fileSocket);
        FileDetailsMessage castResponse = (FileDetailsMessage) response;
        response = null;

        if (castResponse.getStatus() != FileDetailsStatus.FILEDETAILS_START)
            return null;

        // Prepare to receive File Details
        ArrayList<FileInfo> currFileDetails = null;
        ObjectInputStream fromServer = null;
        try {
            currFileDetails = new ArrayList<FileInfo>(Integer.parseInt(castResponse.getHeaders().get("count")));
            fromServer = new ObjectInputStream(fileSocket.getInputStream());

            for (int i = 0; i < currFileDetails.size(); ++i)
                currFileDetails.add((FileInfo) fromServer.readObject());

            return currFileDetails;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            try {
                fromServer.close();
                fileSocket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
