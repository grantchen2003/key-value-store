package io.github.grantchen2003.key.value.store.shard.server;

import io.github.grantchen2003.key.value.store.shard.handlers.LoggingHandler;
import io.github.grantchen2003.key.value.store.shard.handlers.TransactionLogHandler;
import io.github.grantchen2003.key.value.store.shard.handlers.SlaveHandler;
import io.github.grantchen2003.key.value.store.shard.service.MasterService;

import java.io.IOException;

public class MasterServer extends Server {
    protected MasterServer(int port, MasterService masterService) throws IOException {
        super(port, masterService);
        server.createContext("/slave", new LoggingHandler(new SlaveHandler(masterService)));
        server.createContext("/transaction-log", new LoggingHandler(new TransactionLogHandler(masterService)));
    }

    @Override
    public void start() {
        server.start();
        System.out.println("Master shard server started on port " + server.getAddress().getPort());
    }
}
