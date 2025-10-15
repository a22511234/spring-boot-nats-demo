package com.howard0720.natsdemo;

import io.nats.client.Message;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueStatus;
import io.nats.client.api.PublishAck;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/api/nats")
public class NatsController {
    private final JetStreamService jetStreamService;
    private final KvStoreService kvStoreService;

    public NatsController(JetStreamService jetStreamService, KvStoreService kvStoreService) {
        this.jetStreamService = jetStreamService;
        this.kvStoreService = kvStoreService;
    }
    // ==================== JetStream API ====================
    @PostMapping("/jetstream/publish")
    public ResponseEntity<Map<String, Object>> publishMessage(
            @RequestParam String subject,
            @RequestBody String message) {
        try {
            PublishAck ack = jetStreamService.publish(subject, message);
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("subject", subject);
            response.put("sequence", ack.getSeqno());
            response.put("stream", ack.getStream());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error publishing message", e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("success", false, "error", e.getMessage()));
        }
    }

    @PostMapping("/jetstream/subscribe/push")
    public ResponseEntity<Map<String, Object>> subscribePush(
            @RequestParam String subject,
            @RequestParam String consumerName) {
        try {
            jetStreamService.subscribePush(subject, consumerName);
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "message", "Push subscription created",
                    "subject", subject,
                    "consumer", consumerName
            ));
        } catch (Exception e) {
            log.error("Error creating push subscription", e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("success", false, "error", e.getMessage()));
        }
    }

    @GetMapping("/jetstream/pull")
    public ResponseEntity<Map<String, Object>> pullMessages(
            @RequestParam String subject,
            @RequestParam String consumerName,
            @RequestParam(defaultValue = "10") int batchSize) {
        try {
            List<Message> messages = jetStreamService.pullMessages(subject, consumerName, batchSize);
            List<String> messageData = messages.stream()
                    .map(msg -> new String(msg.getData(), StandardCharsets.UTF_8))
                    .collect(Collectors.toList());

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "count", messages.size(),
                    "messages", messageData
            ));
        } catch (Exception e) {
            log.error("Error pulling messages", e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("success", false, "error", e.getMessage()));
        }
    }

    @PostMapping("/jetstream/request")
    public ResponseEntity<Map<String, Object>> requestReply(
            @RequestParam String subject,
            @RequestBody String message) {
        try {
            String reply = jetStreamService.request(subject, message, Duration.ofSeconds(5));
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "reply", reply != null ? reply : "No reply"
            ));
        } catch (Exception e) {
            log.error("Error in request-reply", e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("success", false, "error", e.getMessage()));
        }
    }

    // ==================== KV Store API ====================

    @PutMapping("/kv/{key}")
    public ResponseEntity<Map<String, Object>> putKv(
            @PathVariable String key,
            @RequestBody String value) {
        try {
            long revision = kvStoreService.put(key, value);
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "key", key,
                    "revision", revision
            ));
        } catch (Exception e) {
            log.error("Error putting KV", e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("success", false, "error", e.getMessage()));
        }
    }

    @GetMapping("/kv/{key}")
    public ResponseEntity<Map<String, Object>> getKv(@PathVariable String key) {
        try {
            String value = kvStoreService.get(key);
            if (value != null) {
                return ResponseEntity.ok(Map.of(
                        "success", true,
                        "key", key,
                        "value", value
                ));
            } else {
                return ResponseEntity.notFound().build();
            }
        } catch (Exception e) {
            log.error("Error getting KV", e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("success", false, "error", e.getMessage()));
        }
    }

    @DeleteMapping("/kv/{key}")
    public ResponseEntity<Map<String, Object>> deleteKv(@PathVariable String key) {
        try {
            kvStoreService.delete(key);
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "message", "Key deleted"
            ));
        } catch (Exception e) {
            log.error("Error deleting KV", e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("success", false, "error", e.getMessage()));
        }
    }

    @GetMapping("/kv")
    public ResponseEntity<Map<String, Object>> getAllKeys() {
        try {
            List<String> keys = kvStoreService.getKeys();
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "count", keys.size(),
                    "keys", keys
            ));
        } catch (Exception e) {
            log.error("Error getting keys", e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("success", false, "error", e.getMessage()));
        }
    }

    @GetMapping("/kv/{key}/history")
    public ResponseEntity<Map<String, Object>> getHistory(@PathVariable String key) {
        try {
            List<KeyValueEntry> history = kvStoreService.getHistory(key);
            List<Map<String, Object>> historyData = history.stream()
                    .map(entry -> {
                        Map<String, Object> item = new HashMap<>();
                        item.put("revision", entry.getRevision());
                        item.put("operation", entry.getOperation().toString());
                        if (entry.getValue() != null) {
                            item.put("value", new String(entry.getValue(), StandardCharsets.UTF_8));
                        }
                        item.put("created", entry.getCreated());
                        return item;
                    })
                    .collect(Collectors.toList());

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "key", key,
                    "history", historyData
            ));
        } catch (Exception e) {
            log.error("Error getting history", e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("success", false, "error", e.getMessage()));
        }
    }

    @GetMapping("/kv/status")
    public ResponseEntity<Map<String, Object>> getKvStatus() {
        try {
            KeyValueStatus status = kvStoreService.getStatus();
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "bucket", status.getBucketName(),
                    "entryCount", status.getEntryCount()
            ));
        } catch (Exception e) {
            log.error("Error getting status", e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("success", false, "error", e.getMessage()));
        }
    }
}
