package io.github.grantchen2003.key.value.store.shard.handlers.slave;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.github.grantchen2003.key.value.store.shard.service.SlaveService;
import io.github.grantchen2003.key.value.store.shard.transaction.DeleteTransaction;
import io.github.grantchen2003.key.value.store.shard.transaction.PutTransaction;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;

public class ReplicateHandler implements HttpHandler {
    private final SlaveService slaveService;

    public ReplicateHandler(SlaveService slaveService) {
        this.slaveService = slaveService;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        exchange.sendResponseHeaders(200, 0);

        try (exchange; final ObjectInputStream in = new ObjectInputStream(exchange.getRequestBody())) {
            while (true) {
                final Object obj = in.readObject();

                if (obj instanceof PutTransaction tx) {
                    slaveService.put(tx.offset, tx.key, tx.value);
                    System.out.println("REPLICATOR: Applied PutTransaction [offset: " + tx.offset + ", key: " + tx.key + ", value: " + tx.value + "]");
                } else if (obj instanceof DeleteTransaction tx) {
                    slaveService.delete(tx.offset, tx.key);
                    System.out.println("REPLICATOR: Applied DeleteTransaction [offset: " + tx.offset + ", key: " + tx.key + "]");
                } else {
                    System.out.println("REPLICATOR: Received unknown object type: " + obj.getClass().getName());
                }
            }
        } catch (EOFException e) {
            System.out.println("REPLICATOR: Stream closed by master.");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
