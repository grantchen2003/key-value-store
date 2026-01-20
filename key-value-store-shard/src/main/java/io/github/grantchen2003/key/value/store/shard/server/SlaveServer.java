package io.github.grantchen2003.key.value.store.shard.server;

import io.github.grantchen2003.key.value.store.shard.service.SlaveService;

import java.io.IOException;

public class SlaveServer extends Server {
    final SlaveService slaveService;

    protected SlaveServer(int port, SlaveService service) throws IOException {
        super(port, service);
        slaveService = service;
    }

    @Override
    public void start() {
        server.start();
        System.out.println("Slave shard server started on port " + server.getAddress().getPort());

        slaveService.start();
    }
}
