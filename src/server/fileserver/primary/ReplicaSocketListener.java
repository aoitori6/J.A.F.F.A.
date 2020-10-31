package server.fileserver.primary;

import java.io.IOException;
import java.net.ServerSocket;

final class ReplicaSocketListener implements Runnable {
    private final ServerSocket replicaServerSocket;

    ReplicaSocketListener(ServerSocket replicaServerSocket) {
        this.replicaServerSocket = replicaServerSocket;
    }

    @Override
    public void run() {
        // Listening for new Replica Server connections
        while (!PrimaryFileServer.executionPool.isShutdown()) {
            try {
                PrimaryFileServer.executionPool.execute(new FromReplicaHandler(this.replicaServerSocket.accept()));
            } catch (IOException e) {
                e.printStackTrace();
                continue;
            }
        }

        try {
            this.replicaServerSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
