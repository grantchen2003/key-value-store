package io.github.grantchen2003.key.value.store.shard.replication;

import com.google.gson.stream.JsonReader;
import io.github.grantchen2003.key.value.store.shard.store.Store;
import io.github.grantchen2003.key.value.store.shard.transaction.DeleteTransaction;
import io.github.grantchen2003.key.value.store.shard.transaction.PutTransaction;
import io.github.grantchen2003.key.value.store.shard.transaction.Transaction;
import io.github.grantchen2003.key.value.store.shard.utils.NetworkUtils;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

// TODO: TEST AND PROPERLY IMPLEMENT THE FUNCTIONS, ITS NOT DONE YET
public class SlaveSyncer {
    private final HttpClient client = HttpClient.newHttpClient();
    private final Store store;
    private final InetSocketAddress masterAddress;
    private long lastAppliedTxOffset = 0;

    public SlaveSyncer(Store store, InetSocketAddress masterAddress) {
        this.store = store;
        this.masterAddress = masterAddress;
    }

    // TODO: KEEP SYNCING WITH POLLING UNTIL LAST APPLIED TX TO SLAVE IS THE SAME AS MASTER. THEN ONLY RELY ON MASTER WRITE PROPAGATIONS TO BE IN SYNC
    public void syncWithMaster() {
        final URI masterUri = URI.create("http://" + NetworkUtils.toHostPort(masterAddress) + "/transaction-log?startOffset=" + lastAppliedTxOffset);
        System.out.println("Sending POST request to register with master at " + masterUri);

        final HttpRequest request = HttpRequest.newBuilder()
                .uri(masterUri)
                .GET()
                .build();

        try {
            final HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() != 200) {
                throw new RuntimeException("Failed to sync with master, status code: " + response.statusCode());
            }

            try (
                    final InputStreamReader reader = new InputStreamReader(response.body());
                    final JsonReader jsonReader = new JsonReader(reader)
            ) {

                jsonReader.beginArray();

                while (jsonReader.hasNext()) {
                    jsonReader.beginObject();

                    String type = null;
                    long offset = -1;
                    String key = null;
                    String value = null;

                    while (jsonReader.hasNext()) {
                        String name = jsonReader.nextName();
                        switch (name) {
                            case "type" -> type = jsonReader.nextString();
                            case "offset" -> offset = jsonReader.nextLong();
                            case "key" -> key = jsonReader.nextString();
                            case "value" -> value = jsonReader.nextString();
                            default -> jsonReader.skipValue();
                        }
                    }

                    if (type == null) {
                        throw new IllegalStateException("Transaction type missing in JSON object");
                    }

                    if (offset == -1) {
                        throw new IllegalStateException("Offset type missing in JSON object");
                    }

                    final Transaction tx = switch (type) {
                        case "PUT" -> {
                            if (value == null) throw new IllegalStateException("PUT transaction missing value");
                            yield new PutTransaction(offset, key, value);
                        }
                        case "DELETE" -> new DeleteTransaction(offset, key);
                        default -> throw new IllegalStateException("Unexpected value: " + type);
                    };

                    applyTransaction(tx);

                    jsonReader.endObject();
                }

                jsonReader.endArray();
            }
        } catch (Exception e) {
            throw new RuntimeException("Error syncing slave with master", e);
        }
    }

    public void applyPutTransaction(PutTransaction tx) {
        if (tx.offset + 1 == lastAppliedTxOffset) {
            store.put(tx.key, tx.value);
            lastAppliedTxOffset++;
        } else if (tx.offset + 1 > lastAppliedTxOffset) {
            syncWithMaster();
        }
    }

    public void applyDeleteTransaction(DeleteTransaction tx) {
        if (tx.offset + 1 == lastAppliedTxOffset) {
            store.remove(tx.key);
            lastAppliedTxOffset++;
        } else if (tx.offset + 1 > lastAppliedTxOffset) {
            syncWithMaster();
        }
    }

    private void applyTransaction(Transaction tx) {
        if (tx instanceof PutTransaction putTx) {
            applyPutTransaction(putTx);
        } else if (tx instanceof DeleteTransaction delTx) {
            applyDeleteTransaction(delTx);
        }
    }
}
