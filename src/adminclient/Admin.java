package adminclient;

import java.util.ArrayList;
import java.io.ObjectInputStream;

import client.Client;
import message.*;
import misc.FileInfo;
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

        // Send a FileDetailsRequest to the Auth Server
        // Expect FILEDETAILS_START if file exists and all is successful
        if (!MessageHelpers.sendMessageTo(this.authSocket,
                new FileDetailsMessage(FileDetailsStatus.FILEDETAILS_REQUEST, null, this.name, this.authToken)))
            return null;

        // Parse Response
        Message response = MessageHelpers.receiveMessageFrom(this.authSocket);
        FileDetailsMessage castResponse = (FileDetailsMessage) response;
        response = null;

        if (castResponse.getStatus() != FileDetailsStatus.FILEDETAILS_START)
            return null;

        // Prepare to receive File Details
        ArrayList<FileInfo> currFileDetails = null;
        ObjectInputStream fromServer = null;
        try {
            currFileDetails = new ArrayList<FileInfo>(Integer.parseInt(castResponse.getHeaders().get("count")));
            fromServer = new ObjectInputStream(this.authSocket.getInputStream());

            for (int i = 0; i < currFileDetails.size(); ++i)
                currFileDetails.add((FileInfo) fromServer.readObject());

            return currFileDetails;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
