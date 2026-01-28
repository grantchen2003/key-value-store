package io.github.grantchen2003.key.value.store.shard.replication;

import io.github.grantchen2003.key.value.store.shard.transaction.Transaction;
import io.github.grantchen2003.key.value.store.shard.transaction.TransactionLog;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
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

    @Override
    public void run() {
        System.out.println("STREAMER: Starting replication thread for " + slaveAddress);

        final URL slaveUrl;
        try {
            final URI slaveUri = new URI("http://" + slaveAddress.getHostString() + ":" + slaveAddress.getPort() + "/internal/replicate");
            slaveUrl = slaveUri.toURL();
        } catch (MalformedURLException | URISyntaxException e) {
            System.err.println("STREAMER: Invalid slave address: " + e.getMessage());
            return;
        }

        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) slaveUrl.openConnection();
            connection.setDoOutput(true);
            connection.setRequestMethod("POST");
            connection.setChunkedStreamingMode(0);

            try (final ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream())) {
                System.out.println("STREAMER: Syncing data to " + slaveAddress + " via HTTP Stream");

                while (!Thread.currentThread().isInterrupted()) {
                    lock.lock();
                    try {
                        while (lastSentIndex >= transactionLog.size()) {
                            txAvailable.await();
                        }
                        for (final Transaction tx : transactionLog.getTransactionsStartingFrom(lastSentIndex + 1)) {
                            out.writeObject(tx);
                            out.flush();
                            lastSentIndex++;
                            System.out.println("STREAMER: Sent " + this);
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            }
        } catch (InterruptedException e) {
                System.err.println("STREAMER: Replication interrupted for " + slaveAddress);
                Thread.currentThread().interrupt();

        } catch (IOException e) {
            System.err.println("STREAMER: HTTP stream error with " + slaveAddress + ": " + e.getMessage());

        } finally {
            if (connection != null) {
                try {
                    final int code = connection.getResponseCode();
                    System.out.println("STREAMER: Stream closed with response code: " + code + " for " + slaveAddress);
                } catch (IOException e) {
                    System.out.println("STREAMER: Could not retrieve response code during cleanup");
                }
                connection.disconnect();
            }
        }
    }

    @Override
    public String toString() {
        return String.format("ReplicationStreamer[slave=%s, lastSent=%d, logSize=%d]",
                slaveAddress, lastSentIndex, transactionLog.size());
    }
}
