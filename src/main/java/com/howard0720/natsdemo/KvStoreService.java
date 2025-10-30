package com.howard0720.natsdemo;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueStatus;
import io.nats.client.api.StorageType;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class KvStoreService {
    private final Connection natsConnection;
    private final KeyValue keyValue;

    public KvStoreService(Connection natsConnection, KeyValue keyValue) {
        this.natsConnection = natsConnection;
        this.keyValue = keyValue;
    }

    @PostConstruct
    public void init() throws IOException, InterruptedException {
        // 建立或取得 KV Bucket
        String bucketName = "demo-bucket";
    }

    /**
     * 設定 Key-Value
     */
    public long put(String key, String value) throws IOException, InterruptedException, JetStreamApiException {
        long revision = keyValue.put(key, value.getBytes(StandardCharsets.UTF_8));
        log.info("Put key: {}, value: {}, revision: {}", key, value, revision);
        return revision;
    }

    /**
     * 取得 Value
     */
    public String get(String key) throws IOException, InterruptedException, JetStreamApiException {
        KeyValueEntry entry = keyValue.get(key);
        if (entry != null && entry.getValue() != null) {
            String value = new String(entry.getValue(), StandardCharsets.UTF_8);
            log.info("Get key: {}, value: {}", key, value);
            return value;
        }
        log.info("Key not found: {}", key);
        return null;
    }

    /**
     * 刪除 Key
     */
    public void delete(String key) throws IOException, InterruptedException, JetStreamApiException {
        keyValue.delete(key);
        log.info("Deleted key: {}", key);
    }

    /**
     * 清除 Key (標記為刪除但保留歷史)
     */
    public void purge(String key) throws IOException, InterruptedException, JetStreamApiException {
        keyValue.purge(key);
        log.info("Purged key: {}", key);
    }

    /**
     * 取得所有 Keys
     */
    public List<String> getKeys() throws IOException, InterruptedException, JetStreamApiException {
        List<String> keys = new ArrayList<>();
        keyValue.keys().forEach(keys::add);
        log.info("Retrieved {} keys", keys.size());
        return keys;
    }

    /**
     * 取得 Key 的歷史記錄
     */
    public List<KeyValueEntry> getHistory(String key) throws IOException, InterruptedException, JetStreamApiException {
        List<KeyValueEntry> history = keyValue.history(key);
        log.info("Retrieved {} history entries for key: {}", history.size(), key);
        return history;
    }

    /**
     * 條件式更新 - 只在 revision 匹配時更新
     */
    public long update(String key, String value, long expectedRevision)
            throws IOException, InterruptedException, JetStreamApiException {
        long revision = keyValue.update(key, value.getBytes(StandardCharsets.UTF_8), expectedRevision);
        log.info("Updated key: {}, new revision: {}", key, revision);
        return revision;
    }

    /**
     * 建立 Key (只在不存在時建立)
     */
    public long create(String key, String value) throws IOException, InterruptedException, JetStreamApiException {
        long revision = keyValue.create(key, value.getBytes(StandardCharsets.UTF_8));
        log.info("Created key: {}, revision: {}", key, revision);
        return revision;
    }

    /**
     * 取得 Bucket 狀態
     */
    public KeyValueStatus getStatus() throws IOException, InterruptedException, JetStreamApiException {
        KeyValueStatus status = keyValue.getStatus();
        log.info("Bucket: {}, Entry count: {}, BackingStore: {}, Other metadata: {}",
                status.getBucketName(),
                status.getEntryCount(),
                status.getBackingStore(),
                status.getMetadata());
        return status;
    }

}
