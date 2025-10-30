package com.howard0720.natsdemo;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * 自訂 NATS Testcontainer，支援 JetStream
 */
public class NatsContainer extends GenericContainer<NatsContainer> {

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("nats:2.10-alpine");
    private static final int NATS_PORT = 4222;
    private static final int MONITORING_PORT = 8222;

    public NatsContainer() {
        this(DEFAULT_IMAGE_NAME);
    }

    public NatsContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);

        // 啟用 JetStream 和監控
        withCommand("-js", "-m", String.valueOf(MONITORING_PORT));

        // 暴露端口
        withExposedPorts(NATS_PORT, MONITORING_PORT);

        // 等待容器準備就緒 - 使用多個等待條件
        waitingFor(
                Wait.forLogMessage(".*Server is ready.*", 1)
                        .withStartupTimeout(Duration.ofSeconds(60))
        );

        // 設定啟動超時
        withStartupTimeout(Duration.ofSeconds(60));
    }

    /**
     * 取得 NATS 連線 URL
     */
    public String getNatsUrl() {
        return String.format("nats://%s:%d", getHost(), getMappedPort(NATS_PORT));
    }

    /**
     * 取得監控端口
     */
    public Integer getMonitoringPort() {
        return getMappedPort(MONITORING_PORT);
    }

    /**
     * 取得監控 URL
     */
    public String getMonitoringUrl() {
        return String.format("http://%s:%d", getHost(), getMonitoringPort());
    }
}