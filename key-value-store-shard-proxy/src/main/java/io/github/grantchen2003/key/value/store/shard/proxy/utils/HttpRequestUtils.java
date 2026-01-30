package io.github.grantchen2003.key.value.store.shard.proxy.utils;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class HttpRequestUtils {
    public static Optional<String> getParam(URI uri, String name) {
        return Optional.ofNullable(parseQueryParams(uri).get(name));
    }

    private static Map<String, String> parseQueryParams(URI uri) {
        final Map<String, String> queryParams = new HashMap<>();
        if (uri == null) {
            return queryParams;
        }

        final String query = uri.getQuery();
        if (query == null || query.isEmpty()) {
            return queryParams;
        }

        final String[] pairs = query.split("&");
        for (final String pair : pairs) {
            final int idx = pair.indexOf("=");
            if (idx > 0 && idx < pair.length() - 1) {
                final String key = URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8);
                final String value = URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8);
                queryParams.put(key, value);
            } else if (idx == pair.length() - 1) {
                final String key = URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8);
                queryParams.put(key, "");
            } else {
                queryParams.put(URLDecoder.decode(pair, StandardCharsets.UTF_8), null);
            }
        }

        return queryParams;
    }
}
