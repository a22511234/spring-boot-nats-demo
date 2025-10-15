package com.howard0720.natsdemo;

import io.nats.client.*;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.NatsMessage;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class JetStreamService {
  private final JetStream jetStream;
  private final Connection natsConnection;

  public JetStreamService(JetStream jetStream, Connection natsConnection) {
    this.jetStream = jetStream;
    this.natsConnection = natsConnection;
  }

  /**
   * 發布訊息到 JetStream
   */
  public PublishAck publish(String subject, String message)
      throws IOException, InterruptedException, JetStreamApiException {
    Message msg =
        NatsMessage.builder().subject(subject).data(message, StandardCharsets.UTF_8).build();

    PublishAck ack = jetStream.publish(msg);
    log.info("Published message to {}: {}, seq: {}", subject, message, ack.getSeqno());
    return ack;
  }

  /**
   * Push 訂閱 - 自動推送訊息
   */
  public void subscribePush(String subject, String consumerName)
      throws IOException, InterruptedException, JetStreamApiException {
    PushSubscribeOptions options =
        PushSubscribeOptions.builder()
            .durable(consumerName)
            .configuration(
                ConsumerConfiguration.builder()
                    .ackPolicy(AckPolicy.Explicit)
                    .ackWait(Duration.ofSeconds(30))
                    .build())
            .build();
    jetStream.subscribe(subject, options);

    log.info("Push subscription created for subject: {}", subject);
  }

  /**
   * Pull 訂閱 - 主動拉取訊息
   */
  public List<Message> pullMessages(String subject, String consumerName, int batchSize)
      throws IOException, InterruptedException, TimeoutException, JetStreamApiException {

    PullSubscribeOptions options =
        PullSubscribeOptions.builder()
            .durable(consumerName)
            .configuration(ConsumerConfiguration.builder().ackPolicy(AckPolicy.Explicit).build())
            .build();

    var subscription = jetStream.subscribe(subject, options);

    // 拉取訊息
    List<Message> messages = subscription.fetch(batchSize, Duration.ofSeconds(5));

    for (Message msg : messages) {
      String data = new String(msg.getData(), StandardCharsets.UTF_8);
      log.info("Pulled message: {}", data);
      msg.ack(); // 確認訊息
    }

    return messages;
  }

  /**
   * 發布訊息並等待回應
   */
  public String request(String subject, String message, Duration timeout)
      throws IOException, InterruptedException {

    Message msg =
        NatsMessage.builder().subject(subject).data(message, StandardCharsets.UTF_8).build();

    Message reply = natsConnection.request(msg, timeout);

    if (reply != null) {
      String response = new String(reply.getData(), StandardCharsets.UTF_8);
      log.info("Received reply: {}", response);
      return response;
    }

    return null;
  }
}