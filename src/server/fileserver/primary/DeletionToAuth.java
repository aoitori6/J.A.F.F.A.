package server.fileserver.primary;

import java.net.Socket;

import signals.base.Message;
import signals.derived.delete.DeleteMessage;
import signals.derived.delete.DeleteStatus;
import signals.utils.MessageHelpers;

final class DeletionToAuth implements Runnable {
    private final String code;

    DeletionToAuth(String code) {
        this.code = code;
    }

    @Override
    /**
     * Service responsible for sending DeleteMessage objects to the AuthServer to
     * synchronize deletion.
     */
    public void run() {
        try (Socket toAuth = new Socket(PrimaryFileServer.authServerAddr.getAddress(),
                PrimaryFileServer.authServerAddr.getPort());) {

            if (!MessageHelpers.sendMessageTo(toAuth, new DeleteMessage(DeleteStatus.DELETE_REQUEST, this.code, null,
                    PrimaryFileServer.SERVER_NAME, PrimaryFileServer.SERVER_TOKEN, true)))
                throw new Exception("Couldn't delete File with Code: " + this.code);

            // Getting the response from the Auth Server
            Message response = MessageHelpers.receiveMessageFrom(toAuth);
            DeleteMessage castResponse = (DeleteMessage) response;
            response = null;

            if (castResponse.getStatus() != DeleteStatus.DELETE_SUCCESS)
                throw new Exception("Couldn't delete File with Code: " + this.code);

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("ERROR: Couldn't delete File with Code " + this.code);
        }
    }

}
