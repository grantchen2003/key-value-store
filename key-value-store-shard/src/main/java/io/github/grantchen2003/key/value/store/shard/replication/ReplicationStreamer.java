package io.github.grantchen2003.key.value.store.shard.replication;

import io.github.grantchen2003.key.value.store.shard.transaction.Transaction;
import io.github.grantchen2003.key.value.store.shard.transaction.TransactionLog;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ReplicationStreamer implements Runnable {
    private final InetSocketAddress slaveAddress;
    private final TransactionLog transactionLog;
    private final ReentrantLock lock;
    private final Condition txAvailable;
    private int lastSentIndex = 0;

    public ReplicationStreamer(InetSocketAddress slaveAddress, TransactionLog transactionLog, ReentrantLock lock, Condition txAvailable) {
        this.slaveAddress = slaveAddress;
        this.transactionLog = transactionLog;
        this.lock = lock;
        this.txAvailable = txAvailable;
    }

    // TODO: switch to grpc, make slave be grpc server
    @Override
    public void run() {
        System.out.println("STREAMER: Starting replication thread for " + slaveAddress);

        try (final Socket socket = new Socket()) {
            socket.connect(slaveAddress, 5000);
            socket.setKeepAlive(true);

            try (final ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {
                System.out.println("STREAMER: Syncing data to " + slaveAddress);

                while (!Thread.currentThread().isInterrupted() && !socket.isClosed()) {
                    lock.lock();
                    try {
                        while (lastSentIndex >= transactionLog.size()) {
                            txAvailable.await();
                        }

                        for (final Transaction tx : transactionLog.getTransactionsStartingFrom(lastSentIndex + 1)) {
                            out.writeObject(tx);
                            out.flush();
                            lastSentIndex++;
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            }
        } catch (ConnectException e) {
            System.err.println("STREAMER: Could not connect to slave at " + slaveAddress + " (Connection Refused)");
        } catch (IOException e) {
            System.err.println("STREAMER: Connection error with slave " + slaveAddress + ": " + e.getMessage());
        } catch (InterruptedException e) {
            System.err.println("STREAMER: Replication thread interrupted for " + slaveAddress);
            Thread.currentThread().interrupt();
        } finally {
            System.out.println("STREAMER: Closed replication stream for " + slaveAddress);
        }
    }
}
