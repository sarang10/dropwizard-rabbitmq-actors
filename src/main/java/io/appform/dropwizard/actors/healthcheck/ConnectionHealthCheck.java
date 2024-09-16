package io.appform.dropwizard.actors.healthcheck;

import com.codahale.metrics.health.HealthCheck;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConnectionHealthCheck extends HealthCheck {

  private final Connection connection;
  private final Channel channel;

  public ConnectionHealthCheck(Connection connection, Channel channel) {
    this.connection = connection;
    this.channel = channel;
  }

  @Override
  protected Result check() throws Exception {
    if (connection == null) {
      log.warn("RMQ Healthcheck::No RMQ connection available");
      return Result.unhealthy("No RMQ connection available");
    }
    if (!connection.isOpen()) {
      log.warn("RMQ Healthcheck::RMQ connection is not open");
      return Result.unhealthy("RMQ connection is not open");
    }
    if (null == channel) {
      log.warn("RMQ Healthcheck::Producer channel is down");
      return Result.unhealthy("Producer channel is down");
    }
    if (!channel.isOpen()) {
      log.warn("RMQ Healthcheck::Producer channel is closed");
      return Result.unhealthy("Producer channel is closed");
    }
    return Result.healthy();
  }
}