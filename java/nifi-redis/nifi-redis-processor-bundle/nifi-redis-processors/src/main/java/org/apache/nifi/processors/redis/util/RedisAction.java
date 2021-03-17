package org.apache.nifi.processors.redis.util;


import org.springframework.data.redis.connection.RedisConnection;

import java.io.IOException;

/**
 * An action to be executed with a RedisConnection.
 */
public interface RedisAction<T> {

    T execute(RedisConnection redisConnection) throws IOException;

}