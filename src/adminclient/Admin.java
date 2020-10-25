package adminclient;

import java.util.ArrayList;

import client.Client;

public class Admin extends Client {

    public Admin(String address, int port) {
        super(address, port);
    }

    /**
     * 
     * @return
     */
    public ArrayList<FileInfo> getAllFileData() {
        
        return null;
    }
}
