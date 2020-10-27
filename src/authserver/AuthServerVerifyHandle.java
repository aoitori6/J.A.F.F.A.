package authserver;

import java.net.Socket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import message.AuthMessage;
import message.MessageHelpers;
import statuscodes.AuthStatus;

public class AuthServerVerifyHandle implements Runnable {
    private Socket fileServer;
    private Connection authDB;

    public AuthServerVerifyHandle(Socket fileServer, Connection authDB) {
        this.fileServer = fileServer;
        this.authDB = authDB;
    }

    @Override
    public void run() {
        try {
            AuthMessage received;
            PreparedStatement query;
            ResultSet queryResp;

            while (!fileServer.isClosed() && !authDB.isClosed()) {
                received = (AuthMessage) MessageHelpers.receiveMessageFrom(this.fileServer);

                if (received.getStatus() != AuthStatus.AUTH_CHECK)
                    throw new IllegalArgumentException();

                query = authDB.prepareStatement(
                        "SELECT Auth_Code FROM client WHERE Username = ? AND Login_Status = 'ONLINE'");
                query.setString(1, received.getClientName());
                queryResp = query.executeQuery();

                if (queryResp.next()) {
                    if (queryResp.getString("Auth_Code").equals(received.getAuthToken())) {
                        MessageHelpers.sendMessageTo(this.fileServer,
                                new AuthMessage(AuthStatus.AUTH_VALID, null, "Auth Server", received.getClientName(),
                                        received.getAuthToken(), queryResp.getBoolean("Admin_Status")));
                    }
                }

                else
                    MessageHelpers.sendMessageTo(this.fileServer, new AuthMessage(AuthStatus.AUTH_INVALID, null,
                            "Auth Server", received.getClientName(), received.getAuthToken(), false));

                query.close();
            }

        } catch (Exception e) {

            e.printStackTrace();
            MessageHelpers.sendMessageTo(this.fileServer,
                    new AuthMessage(AuthStatus.AUTH_CHECKFAIL, null, "Auth Server", null, null, false));
            return;

        } finally {

            try {
                this.authDB.close();
                this.fileServer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}
