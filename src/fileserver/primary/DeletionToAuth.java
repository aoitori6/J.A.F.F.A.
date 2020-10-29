package fileserver.primary;

import java.net.InetSocketAddress;
import java.net.Socket;

import message.DeleteMessage;
import message.Message;
import message.MessageHelpers;
import statuscodes.DeleteStatus;

final class DeletionToAuth implements Runnable {
    private final InetSocketAddress authServerAddr;
    private final String code;

    DeletionToAuth(InetSocketAddress authServerAddr, String code) {
        this.authServerAddr = authServerAddr;
        this.code = code;
    }

    @Override
    public void run() {
        try (Socket toAuth = new Socket(this.authServerAddr.getAddress(), this.authServerAddr.getPort());) {

            if (!MessageHelpers.sendMessageTo(toAuth, new DeleteMessage(DeleteStatus.DELETE_REQUEST, this.code, null,
                    "Replica Server", "tempToken", true)))
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
