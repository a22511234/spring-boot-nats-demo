package com.howard0720.natsdemo;

import io.nats.client.*;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.time.Duration;

@Configuration
@Slf4j
public class NatsConfig {
    @Value("${nats.server.url:nats://localhost:4222}")
    private String natsUrl;

    @Value("${nats.stream.name:DEMO_STREAM}")
    private String streamName;

    @Value("${nats.stream.subjects:demo.>}")
    private String streamSubjects;

    private Connection natsConnection;

    @Bean
    public Connection natsConnection() throws IOException, InterruptedException {
        Options options = new Options.Builder()
                .server(natsUrl)
                .connectionTimeout(Duration.ofSeconds(5))
                .pingInterval(Duration.ofSeconds(10))
                .reconnectWait(Duration.ofSeconds(1))
                .maxReconnects(-1)
                .connectionListener(new ConnectionListener() {
                    @Override
                    public void connectionEvent(Connection connection, Events events) {
                        log.info(events.toString());
                    }
                })
                .build();

        natsConnection = Nats.connect(options);
        log.info("Connected to NATS server: {}", natsUrl);

        return natsConnection;
    }

    @Bean
    public JetStream jetStream(Connection connection) throws IOException {
        JetStream jetStream = connection.jetStream();
        log.info("JetStream initialized");
        return jetStream;
    }

    @Bean
    public KeyValue keyValue() {
        String bucketName = "demo-bucket";
        try {
            KeyValueConfiguration config = KeyValueConfiguration.builder()
                    .name(bucketName)
                    .storageType(StorageType.File)
                    .maxHistoryPerKey(10) // 每個 key 保留 10 個歷史版本
                    .ttl(Duration.ofHours(24)) // 24 小時過期
                    .build();

            natsConnection.keyValueManagement().create(config);
            KeyValue keyValue = natsConnection.keyValue(bucketName);
            log.info("KV Bucket '{}' created", bucketName);
            return keyValue;
        } catch (Exception e) {
            log.info("KV Bucket '{}' already exists", bucketName);
        }
        return null;
    }

    @Bean
    public JetStreamManagement jetStreamManagement(Connection connection) throws IOException {
        JetStreamManagement jsm = connection.jetStreamManagement();

        // 建立 Stream (如果不存在)
        try {
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .name(streamName)
                    .subjects(streamSubjects)
                    .storageType(StorageType.File)
                    .retentionPolicy(RetentionPolicy.Limits)
                    .maxAge(Duration.ofDays(7))
                    .build();

            jsm.addStream(streamConfig);
            log.info("Stream '{}' created with subjects '{}'", streamName, streamSubjects);
        } catch (Exception e) {
            log.info("Stream '{}' already exists or error: {}", streamName, e.getMessage());
        }

        return jsm;
    }
}
