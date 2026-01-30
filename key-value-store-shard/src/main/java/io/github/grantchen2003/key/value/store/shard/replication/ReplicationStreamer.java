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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ReplicationStreamer implements Runnable {
    @FunctionalInterface
    public interface SlaveDeathCallback {
        void onSlaveDeath(InetSocketAddress slaveAddress);
    }

    private final InetSocketAddress slaveAddress;
    private final SlaveDeathCallback slaveDeathCallback;
    private final TransactionLog transactionLog;
    private final ReentrantLock lock;
    private final Condition txAvailable;
    private int lastSentIndex = 0;

    private static final int MAX_RETRIES = 4;
    private static final long INITIAL_BACKOFF_MS = 500;
    private static final int BACKOFF_MULTIPLIER = 5;

    public ReplicationStreamer(InetSocketAddress slaveAddress, SlaveDeathCallback slaveDeathCallback, TransactionLog transactionLog, ReentrantLock lock, Condition txAvailable) {
        this.slaveAddress = slaveAddress;
        this.slaveDeathCallback = slaveDeathCallback;
        this.transactionLog = transactionLog;
        this.lock = lock;
        this.txAvailable = txAvailable;
    }

    @Override
    public void run() {
        System.out.println("STREAMER: Starting replication thread for " + slaveAddress);

        final URL slaveUrl;
        try {
            final URI slaveUri = new URI("http://" + slaveAddress.getHostString() + ":" + slaveAddress.getPort() + "/replicate");
            slaveUrl = slaveUri.toURL();
        } catch (MalformedURLException | URISyntaxException e) {
            System.err.println("STREAMER: Invalid slave address: " + e.getMessage());
            return;
        }

        long backoffMs = INITIAL_BACKOFF_MS;
        int consecutiveFailures = 0;

        while (!Thread.currentThread().isInterrupted()) {
            HttpURLConnection connection = null;
            try {
                connection = (HttpURLConnection) slaveUrl.openConnection();
                connection.setDoOutput(true);
                connection.setRequestMethod("POST");
                connection.setChunkedStreamingMode(0);

                try (final ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream())) {
                    System.out.println("STREAMER: Syncing data to " + slaveAddress + " via HTTP Stream");

                    consecutiveFailures = 0;
                    backoffMs = INITIAL_BACKOFF_MS;

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
                consecutiveFailures++;

                if (consecutiveFailures >= MAX_RETRIES) {
                    System.err.println(
                            "STREAMER: Slave " + slaveAddress +
                                    " marked DEAD after " + consecutiveFailures + " failures"
                    );

                    slaveDeathCallback.onSlaveDeath(slaveAddress);
                    return;
                }

                System.err.println(
                        "STREAMER: IO error with " + slaveAddress +
                                " (attempt " + consecutiveFailures + "/" + MAX_RETRIES + "), " +
                                "retrying in " + backoffMs + " ms"
                );

                try {
                    TimeUnit.MILLISECONDS.sleep(backoffMs);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }

                backoffMs *= BACKOFF_MULTIPLIER;

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
    }

    @Override
    public String toString() {
        return String.format("ReplicationStreamer[slave=%s, lastSent=%d, logSize=%d]",
                slaveAddress, lastSentIndex, transactionLog.size());
    }
}
