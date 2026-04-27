package com.example.orders.service;

import com.example.orders.config.RedisStreamsProperties;
import com.example.orders.model.OrderCreatedEvent;
import com.example.orders.redis.OrderEventJsonCodec;
import com.example.orders.support.StreamNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;



@Service
public class NotificationStreamConsumer implements StreamListener<String, MapRecord<String, String, String>> {
    private static final Logger log = LoggerFactory.getLogger(NotificationStreamConsumer.class);

    private final RedisTemplate<String, String> redisTemplate;

    private final RedisStreamsProperties properties;

    private final OrderEventJsonCodec codec;

    public NotificationStreamConsumer(
            RedisTemplate<String, String> redisTemplate,
            RedisStreamsProperties properties,
            OrderEventJsonCodec codec
    ) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.codec = codec;
    }

    @Override
    public void onMessage(MapRecord<String, String, String> record) {
        handleRecord(record, false);
    }

    private void handleRecord(MapRecord<String, String, String> record, boolean claimed) {
        RecordId recordId = record.getId();
        log.info(
                "Handling Redis Stream record: recordId={}, claimed={}",
                recordId,
                claimed
        );

        try {

            String payload = extractPayload(record);

            OrderCreatedEvent event = codec.decode(payload);

            processEvent(event);

            acknowledge(recordId);
            deleteRetryCounter(recordId);

            log.info(
                    "Record processed successfully and acknowledged: recordId={}",
                    recordId
            );
        } catch (Exception ex) {
            handleProcessingError(record, ex);
        }
    }


    private String extractPayload(MapRecord<String, String, String> record) {
        String payload = record.getValue().get(StreamNames.PAYLOAD_FIELD);

        if (payload == null || payload.isBlank()) {
            throw new IllegalArgumentException("Redis Stream record does not contain payload field");
        }
        return payload;
    }

    private void processEvent(OrderCreatedEvent event) {
        log.info(
                "Order received: orderId={}, userId={}, productCode={}, quantity={}",
                event.orderId(),
                event.userId(),
                event.productCode(),
                event.quantity()
        );

        if (event.quantity() > 100) {
            throw new IllegalStateException(
                    "Simulated notification error: quantity is greater than 100"
            );
        }

        log.info(
                "Notification sent successfully for orderId={}",
                event.orderId()
        );
    }

    private void acknowledge(RecordId recordId) {
        Long acknowledgedCount = redisTemplate
                .opsForStream()
                .acknowledge(
                        properties.ordersStream(),
                        properties.consumerGroup(),
                        recordId
                );

        log.info(
                "XACK completed: stream={}, group={}, recordId={}, acknowledgedCount={}",
                properties.ordersStream(),
                properties.consumerGroup(),
                recordId,
                acknowledgedCount
        );
    }


    private void handleProcessingError(MapRecord<String, String, String> record, Exception ex) {

        RecordId recordId = record.getId();


        long attempts = incrementRetryCounter(recordId);

        log.warn(
                "Error while processing recordId={}, attempt={}/{}, error={}",
                recordId,
                attempts,
                properties.maxRetryAttempts(),
                ex.getMessage()
        );

        if (attempts >= properties.maxRetryAttempts()) {

            sendToDlqAndAckOriginal(record, ex);
            return;
        }

        log.info(
                "Record left pending for retry: recordId={}",
                recordId
        );
    }

    @Scheduled(fixedDelayString = "${app.redis-streams.pending-check-delay-ms}")
    public void retryPendingMessages() {
        PendingMessages pendingMessages = redisTemplate
                .opsForStream()
                .pending(
                        properties.ordersStream(),
                        properties.consumerGroup(),
                        Range.unbounded(),
                        properties.pendingBatchSize()
                );

        if (pendingMessages.isEmpty()) {
            return;
        }

        log.info(
                "Found pending Redis Stream messages: count={}",
                pendingMessages.size()
        );


        for (PendingMessage pendingMessage : pendingMessages) {

            retrySinglePendingMessage(pendingMessage);
        }
    }

    private void retrySinglePendingMessage(PendingMessage pendingMessage) {

        RecordId recordId = pendingMessage.getId();

        long attempts = getRetryCounter(recordId);

        if (attempts >= properties.maxRetryAttempts()) {
            log.warn(
                    "Pending record already reached max retry attempts: recordId={}, attempts={}",
                    recordId,
                    attempts
            );
        }

        StreamOperations<String, String, String> streamOperations = redisTemplate.opsForStream();

        List<MapRecord<String, String, String>> claimedRecords = streamOperations.claim(
                properties.ordersStream(),
                properties.consumerGroup(),
                properties.consumerName(),
                Duration.ofMillis(properties.claimIdleTimeMs()),
                recordId
        );

        if (claimedRecords.isEmpty()) {
            log.debug(
                    "No record claimed for pending recordId={}",
                    recordId
            );
            return;
        }

        for (MapRecord<String, String, String> claimedRecord : claimedRecords) {
            if (attempts >= properties.maxRetryAttempts()) {
                sendToDlqAndAckOriginal(
                        claimedRecord,
                        new IllegalStateException("Max retry attempts already reached")
                );
                continue;
            }

            log.info(
                    "Retrying claimed pending record: recordId={}, currentAttempts={}",
                    claimedRecord.getId(),
                    attempts
            );

            handleRecord(claimedRecord, true);
        }
    }

    private void sendToDlqAndAckOriginal(MapRecord<String, String, String> originalRecord, Exception ex) {
        RecordId originalRecordId = originalRecord.getId();
        String payload = originalRecord.getValue().get(StreamNames.PAYLOAD_FIELD);

        if (payload == null) {
            payload = "";
        }

        Map<String, String> dlqBody = Map.of(
                StreamNames.PAYLOAD_FIELD,
                payload,
                StreamNames.ERROR_FIELD,
                ex.getMessage() == null ? ex.getClass().getName() : ex.getMessage(),

                StreamNames.FAILED_AT_FIELD,
                Instant.now().toString(),
                StreamNames.ORIGINAL_RECORD_ID_FIELD,
                originalRecordId.getValue()
        );

        RecordId dlqRecordId = redisTemplate
                .opsForStream()
                .add(
                        StreamRecords
                                .mapBacked(dlqBody)
                                .withStreamKey(properties.dlqStream())
                );

        log.error(
                "Record moved to DLQ: originalRecordId={}, dlqRecordId={}, dlqStream={}",
                originalRecordId,
                dlqRecordId,
                properties.dlqStream()
        );

        acknowledge(originalRecordId);

        deleteRetryCounter(originalRecordId);
    }

    private long incrementRetryCounter(RecordId recordId) {

        String key = retryCounterKey(recordId);

        Long value = redisTemplate.opsForValue().increment(key);

        redisTemplate.expire(key, Duration.ofDays(1));

        return value == null ? 1L : value;
    }

    private long getRetryCounter(RecordId recordId) {
        String value = redisTemplate.opsForValue().get(retryCounterKey(recordId));
        if (value == null) {
            return 0L;
        }

        return Long.parseLong(value);
    }

    private void deleteRetryCounter(RecordId recordId) {
        redisTemplate.delete(retryCounterKey(recordId));
    }

    private String retryCounterKey(RecordId recordId) {
        return properties.ordersStream() + ":retry:" + recordId.getValue();
    }
}
