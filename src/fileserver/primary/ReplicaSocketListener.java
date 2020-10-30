package fileserver.primary;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.sql.Connection;
import java.util.concurrent.ExecutorService;

final class ReplicaSocketListener implements Runnable {
    private final ServerSocket replicaServerSocket;
    private final ExecutorService executionPool;
    private final Connection fileDB;
    private final InetSocketAddress authServerAddr;

    ReplicaSocketListener(ServerSocket replicaServerSocket, ExecutorService executionPool, Connection fileDB,
            InetSocketAddress authServerAddr) {
        this.replicaServerSocket = replicaServerSocket;
        this.executionPool = executionPool;
        this.fileDB = fileDB;
        this.authServerAddr = authServerAddr;
    }

    @Override
    public void run() {
        // Listening for new Replica Server connections
        while (!executionPool.isShutdown()) {
            try {
                this.executionPool.execute(new FromReplicaHandler(this.replicaServerSocket.accept(),
                        this.authServerAddr, this.fileDB, this.executionPool));
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
