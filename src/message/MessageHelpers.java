package message;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class MessageHelpers {
    /**
     * Function that sends a Message object to a Destination Socket
     * 
     * @param destination Socket to Destination Server
     * @param request     Message Object to be sent
     * @return {@code true} if Message was successfully sent to Destination,
     *         {@code false} otherwise
     */
    public static boolean sendMessageTo(Socket destination, Message request) {
        ObjectOutputStream toDestination;
        try {
            toDestination = new ObjectOutputStream(destination.getOutputStream());
            toDestination.writeObject(request);
            toDestination.flush();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            toDestination = null;
        }
    }

    /**
     * Function that receives a Message object from a Source Socket
     * 
     * @param source Socket to Source Server
     * @return {@code Message} object receives from Source Socket if connection was
     *         successful, otherwise {@code null}
     */
    public static Message receiveMessageFrom(Socket source) {
        ObjectInputStream fromDestination;
        Message response;
        try {
            fromDestination = new ObjectInputStream(source.getInputStream());
            response = (Message) fromDestination.readObject();
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            fromDestination = null;
        }
    }
}
