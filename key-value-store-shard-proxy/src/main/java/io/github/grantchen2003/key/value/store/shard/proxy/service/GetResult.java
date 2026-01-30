package io.github.grantchen2003.key.value.store.shard.proxy.service;

import java.util.Optional;

public record GetResult(int statusCode, Optional<String> valueOpt) {}
