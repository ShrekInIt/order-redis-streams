package com.example.orders.redis;

import com.example.orders.config.RedisStreamsProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class RedisStreamInitializer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(RedisStreamInitializer.class);

    private final RedisConnectionFactory connectionFactory;

    private final RedisStreamsProperties properties;

    public RedisStreamInitializer(
            RedisConnectionFactory connectionFactory,
            RedisStreamsProperties properties
    ) {
        this.connectionFactory = connectionFactory;

        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) {
        createConsumerGroupIfNeeded();
    }

    private void createConsumerGroupIfNeeded() {
        try (RedisConnection connection = connectionFactory.getConnection()) {

            byte[] streamKey = properties.ordersStream().getBytes(StandardCharsets.UTF_8);

            connection.streamCommands().xGroupCreate(
                    streamKey,

                    properties.consumerGroup(),

                    ReadOffset.from("0-0"),
                    true
            );

            log.info(
                    "Created Redis Stream group: stream={}, group={}",
                    properties.ordersStream(),
                    properties.consumerGroup()
            );
        } catch (RedisSystemException ex) {

            if (isBusyGroupError(ex)) {
                log.info(
                        "Redis Stream group already exists: stream={}, group={}",
                        properties.ordersStream(),
                        properties.consumerGroup()
                );
                return;
            }
            throw ex;
        } catch (DataAccessException ex) {
            if (isBusyGroupError(ex)) {
                log.info(
                        "Redis Stream group already exists: stream={}, group={}",
                        properties.ordersStream(),
                        properties.consumerGroup()
                );
                return;
            }
            throw ex;
        }
    }

    private boolean isBusyGroupError(Exception ex) {
        String message = ex.getMessage();
        return message != null && message.contains("BUSYGROUP");
    }
}
