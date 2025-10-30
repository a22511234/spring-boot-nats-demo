package com.howard0720.natsdemo;

import io.nats.client.*;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueStatus;
import io.nats.client.api.StorageType;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static io.nats.client.api.KeyValueOperation.PURGE;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/**
 * KV Store 單元測試，使用 Testcontainers
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KvStoreServiceTest {

    @Container
    static NatsContainer natsContainer = new NatsContainer();

    private static Connection connection;
    private static KeyValue keyValue;
    private static final String BUCKET_NAME = "test-bucket";

    @BeforeAll
    static void setUp() throws IOException, InterruptedException {
        // 連線到 NATS
        Options options = new Options.Builder()
                .server(natsContainer.getNatsUrl())
                .connectionTimeout(Duration.ofSeconds(5))
                .maxReconnects(10)
                .reconnectWait(Duration.ofSeconds(1))
                .build();

        connection = Nats.connect(options);
        System.out.println("Connected to NATS: " + natsContainer.getNatsUrl());

        // 等待 JetStream 準備就緒
        Thread.sleep(2000);

        // 建立 KV Bucket
        try {
            KeyValueConfiguration config = KeyValueConfiguration.builder()
                    .name(BUCKET_NAME)
                    .storageType(StorageType.Memory)
                    .maxHistoryPerKey(10)
                    .ttl(Duration.ofHours(1))
                    .build();
            connection.keyValueManagement().create(config);
            keyValue = connection.keyValue(BUCKET_NAME);
            System.out.println("Created KV Bucket: " + BUCKET_NAME);
        } catch (Exception e) {
            // 如果 bucket 已存在，直接取得
            System.out.println("Bucket might exist, trying to get: " + e.getMessage());
            try {
                keyValue = connection.keyValue(BUCKET_NAME);
                System.out.println("Retrieved existing KV Bucket: " + BUCKET_NAME);
            } catch (Exception e2) {
                System.err.println("Failed to get KV Bucket: " + e2.getMessage());
                throw e2;
            }
        }

        // 確認 keyValue 不是 null
        if (keyValue == null) {
            throw new IllegalStateException("KeyValue is null after initialization");
        }
    }

    @AfterAll
    static void tearDown() throws InterruptedException {
        if (connection != null) {
            connection.close();
        }
    }

    @BeforeEach
    void cleanUp() throws IOException, InterruptedException, JetStreamApiException {
        // 清空所有 keys
        List<String> keys = keyValue.keys();
        for (String key : keys) {
            keyValue.purge(key);
        }
    }

    @Test
    @Order(1)
    @DisplayName("測試 Put 和 Get 操作")
    void testPutAndGet() throws IOException, InterruptedException, JetStreamApiException {
        // Given
        String key = "user-1";
        String value = "John Doe";

        // When
        long revision = keyValue.put(key, value.getBytes(StandardCharsets.UTF_8));

        // Then
        assertThat(revision).isGreaterThan(0);

        KeyValueEntry entry = keyValue.get(key);
        assertThat(entry).isNotNull();
        assertThat(entry.getKey()).isEqualTo(key);
        assertThat(new String(entry.getValue(), StandardCharsets.UTF_8)).isEqualTo(value);
        assertThat(entry.getRevision()).isEqualTo(revision);
    }

    @Test
    @Order(2)
    @DisplayName("測試更新操作")
    void testUpdate() throws IOException, InterruptedException, JetStreamApiException {
        // Given
        String key = "user-2";
        String value1 = "Alice";
        String value2 = "Alice Smith";

        // When
        long revision1 = keyValue.put(key, value1.getBytes(StandardCharsets.UTF_8));
        long revision2 = keyValue.put(key, value2.getBytes(StandardCharsets.UTF_8));

        // Then
        assertThat(revision2).isGreaterThan(revision1);

        KeyValueEntry entry = keyValue.get(key);
        assertThat(new String(entry.getValue(), StandardCharsets.UTF_8)).isEqualTo(value2);
        assertThat(entry.getRevision()).isEqualTo(revision2);
    }

    @Test
    @Order(3)
    @DisplayName("測試條件式更新")
    void testConditionalUpdate() throws IOException, InterruptedException, JetStreamApiException {
        // Given
        String key = "user-3";
        String value1 = "Bob";
        String value2 = "Bob Johnson";

        long revision1 = keyValue.create(key, value1.getBytes(StandardCharsets.UTF_8));

        // When - 使用正確的 revision 更新
        long revision2 = keyValue.update(key, value2.getBytes(StandardCharsets.UTF_8), revision1);

        // Then
        assertThat(revision2).isGreaterThan(revision1);

        // When - 使用錯誤的 revision 更新
        assertThatThrownBy(() ->
                keyValue.update(key, "New Value".getBytes(StandardCharsets.UTF_8), revision1)
        ).isInstanceOf(Exception.class);
    }

    @Test
    @Order(4)
    @DisplayName("測試 Create 操作 (僅在不存在時建立)")
    void testCreate() throws IOException, InterruptedException, JetStreamApiException {
        // Given
        String key = "user-4";
        String value = "Charlie";

        // When - 第一次建立
        long revision = keyValue.create(key, value.getBytes(StandardCharsets.UTF_8));

        // Then
        assertThat(revision).isGreaterThan(0);

        // When - 嘗試再次建立同一個 key
        assertThatThrownBy(() ->
                keyValue.create(key, "Another Value".getBytes(StandardCharsets.UTF_8))
        ).isInstanceOf(Exception.class);
    }

    @Test
    @Order(5)
    @DisplayName("測試 Delete 操作")
    void testDelete() throws IOException, InterruptedException, JetStreamApiException {
        // Given
        String key = "user-5";
        String value = "David";
        keyValue.put(key, value.getBytes(StandardCharsets.UTF_8));

        // When
        keyValue.delete(key);

        // Then
        KeyValueEntry entry = keyValue.get(key);
        assertThat(entry).isNull();
    }

    @Test
    @Order(6)
    @DisplayName("測試 Purge 操作")
    void testPurge() throws IOException, InterruptedException, JetStreamApiException {
        // Given
        String key = "user-6";
        keyValue.purge(key);

        // Then
        KeyValueEntry entry = keyValue.get(key);
        assertThat(entry).isNull();

        // Purge 會在歷史記錄中留下一個 PURGE 操作記錄
        List<KeyValueEntry> history = keyValue.history(key);
        assertThat(history).isNotNull(); // 至少有一個 PURGE 記錄

        // 檢查最後一個操作是 PURGE
        KeyValueEntry lastEntry = history.get(history.size() - 1);
        assertThat(lastEntry.getOperation()).isEqualTo(PURGE);
        assertThat(lastEntry.getValue()).isNull();
    }

    @Test
    @Order(7)
    @DisplayName("測試取得所有 Keys")
    void testGetKeys() throws IOException, InterruptedException, JetStreamApiException {
        // Given
        keyValue.put("user-7", "User7".getBytes(StandardCharsets.UTF_8));
        keyValue.put("user-8", "User8".getBytes(StandardCharsets.UTF_8));
        keyValue.put("user-9", "User9".getBytes(StandardCharsets.UTF_8));

        // When
        List<String> keys = keyValue.keys();

        // Then
        assertThat(keys.size()).isEqualTo(3);
    }

    @Test
    @Order(8)
    @DisplayName("測試歷史記錄")
    void testHistory() throws IOException, InterruptedException, JetStreamApiException {
        // Given
        String key = "user-10";
        keyValue.put(key, "V1".getBytes(StandardCharsets.UTF_8));
        keyValue.put(key, "V2".getBytes(StandardCharsets.UTF_8));
        keyValue.put(key, "V3".getBytes(StandardCharsets.UTF_8));

        // When
        List<KeyValueEntry> history = keyValue.history(key);

        // Then
        assertThat(history.size()).isEqualTo(3);
        assertThat(new String(history.get(0).getValue(), StandardCharsets.UTF_8)).isEqualTo("V1");
        assertThat(new String(history.get(1).getValue(), StandardCharsets.UTF_8)).isEqualTo("V2");
        assertThat(new String(history.get(2).getValue(), StandardCharsets.UTF_8)).isEqualTo("V3");
    }

    @Test
    @Order(9)
    @DisplayName("測試 Bucket 狀態")
    void testBucketStatus() throws IOException, InterruptedException, JetStreamApiException {
        // Given
        keyValue.put("user-11", "User11".getBytes(StandardCharsets.UTF_8));

        // When
        KeyValueStatus status = keyValue.getStatus();

        // Then
        assertThat(status).isNotNull();
        assertThat(status.getBucketName()).isEqualTo(BUCKET_NAME);
        assertThat(status.getEntryCount()).isGreaterThan(0);
    }

    @Test
    @Order(10)
    @DisplayName("測試不存在的 Key")
    void testGetNonExistentKey() throws IOException, InterruptedException, JetStreamApiException {
        // When
        KeyValueEntry entry = keyValue.get("non-existent-key");

        // Then
        assertThat(entry).isNull();
    }

    @Test
    @Order(11)
    @DisplayName("測試空值處理")
    void testEmptyValue() throws IOException, JetStreamApiException {
        // Given
        String key = "user-15";
        byte[] emptyValue = new byte[0];

        // When
        long revision = keyValue.put(key, emptyValue);

        // Then
        KeyValueEntry entry = keyValue.get(key);
        assertThat(entry).isNotNull();
        assertThat(entry.getValue()).isNull();
    }
}
