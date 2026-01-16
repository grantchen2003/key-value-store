package io.github.grantchen2003.key.value.store.shard;

import java.util.Map;

public class Config {
    public static Map<String, String> parseArgs(String[] args) {
        String role = null;
        String masterAddress = null;

        for (final String arg : args) {
            if (arg.startsWith("--role==")) {
                role = arg.substring("--role==".length());
            } else if (arg.startsWith("--masterAddress==")) {
                masterAddress = arg.substring("--masterAddress==".length());
            }
        }

        if (role == null) {
            throw new RuntimeException("Missing --role argument");
        }

        if (masterAddress == null) {
            throw new RuntimeException("Missing --masterAddress argument");
        }

        return Map.of(
                "role", role,
                "masterAddress", masterAddress
        );
    }
}
