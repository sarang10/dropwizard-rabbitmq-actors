package io.appform.dropwizard.actors.actor;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import com.rabbitmq.client.AMQP;
import io.appform.dropwizard.actors.base.UnmanagedConsumer;
import io.appform.dropwizard.actors.base.UnmanagedPublisher;
import io.appform.dropwizard.actors.common.Constants;
import io.appform.dropwizard.actors.common.RMQBundleDataProvider;
import io.appform.dropwizard.actors.connectivity.strategy.ConnectionIsolationStrategy;
import io.appform.dropwizard.actors.connectivity.strategy.ConnectionIsolationStrategyVisitor;
import io.appform.dropwizard.actors.connectivity.strategy.DefaultConnectionStrategy;
import io.appform.dropwizard.actors.connectivity.strategy.SharedConnectionStrategy;
import java.util.Set;
import lombok.val;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.NotImplementedException;

public class MultiTenantedActor<Message> {
  private final UnmanagedPublisher<Message> publishActor;
  private final UnmanagedConsumer<Message> consumeActor;
  private final Set<Class<?>> droppedExceptionTypes;
  private final MultiTenantedUnmanagedBaseActor multiTenantedUnmanagedBaseActor;

  protected MultiTenantedActor(String tenantId, ActorConfig config,
      MultiTenantedUnmanagedBaseActor multiTenantedUnmanagedBaseActor) {
    val connectionRegistry = RMQBundleDataProvider.getMultiTenantedConnectionRegistry();
    val consumerConnection = connectionRegistry.createOrGet(tenantId,
        consumerConnectionName(config.getConsumer()));
    val producerConnection = connectionRegistry.createOrGet(tenantId,
        producerConnectionName(config.getProducer()));
    this.multiTenantedUnmanagedBaseActor = multiTenantedUnmanagedBaseActor;
    this.droppedExceptionTypes = multiTenantedUnmanagedBaseActor.getDroppedExceptionTypes();
    this.publishActor = new UnmanagedPublisher<>(multiTenantedUnmanagedBaseActor.getType().name(), config, producerConnection,
        RMQBundleDataProvider.getObjectMapper());
    this.consumeActor = new UnmanagedConsumer<>(multiTenantedUnmanagedBaseActor.getType().name(), config, consumerConnection,
        RMQBundleDataProvider.getObjectMapper(), RMQBundleDataProvider.getRetryStrategyFactory(),
        RMQBundleDataProvider.getExceptionHandlingFactory(), multiTenantedUnmanagedBaseActor.getClazz(),
        multiTenantedUnmanagedBaseActor::handle,
        multiTenantedUnmanagedBaseActor::handleExpiredMessages,
        this::isExceptionIgnorable);
  }

  public void start() throws Exception {
    multiTenantedUnmanagedBaseActor.start();
    if (nonNull(publishActor)) {
      publishActor.start();
    }
    if (nonNull(consumeActor)) {
      consumeActor.start();
    }
  }

  public void stop() throws Exception {
    multiTenantedUnmanagedBaseActor.stop();
    if (nonNull(publishActor)) {
      publishActor.stop();
    }
    if (nonNull(consumeActor)) {
      consumeActor.stop();
    }
  }

  protected boolean isExceptionIgnorable(Throwable t) {
    return droppedExceptionTypes
        .stream()
        .anyMatch(exceptionType -> ClassUtils.isAssignable(t.getClass(), exceptionType));
  }

  public final void publishWithDelay(final Message message, final long delayMilliseconds) throws Exception {
    publishActor().publishWithDelay(message, delayMilliseconds);
  }

  public final void publishWithExpiry(final Message message, final long expiryInMs) throws Exception {
    publishActor().publishWithExpiry(message, expiryInMs);
  }

  public final void publish(final Message message) throws Exception {
    publishActor().publish(message);
  }

  public final void publish(final Message message, final AMQP.BasicProperties properties) throws Exception {
    publishActor().publish(message, properties);
  }

  public final long pendingMessagesCount() {
    return publishActor().pendingMessagesCount();
  }

  public final long pendingSidelineMessagesCount() {
    return publishActor().pendingSidelineMessagesCount();
  }

  private UnmanagedPublisher<Message> publishActor() {
    if (isNull(publishActor)) {
      throw new NotImplementedException("PublishActor is not initialized");
    }
    return publishActor;
  }

  private String producerConnectionName(ProducerConfig producerConfig) {
    if (producerConfig == null) {
      return Constants.DEFAULT_PRODUCER_CONNECTION_NAME;
    }
    return deriveConnectionName(producerConfig.getConnectionIsolationStrategy(), Constants.DEFAULT_PRODUCER_CONNECTION_NAME);
  }

  private String consumerConnectionName(ConsumerConfig consumerConfig) {
    if (consumerConfig == null) {
      return Constants.DEFAULT_CONSUMER_CONNECTION_NAME;
    }

    return deriveConnectionName(consumerConfig.getConnectionIsolationStrategy(), Constants.DEFAULT_CONSUMER_CONNECTION_NAME);
  }

  private String deriveConnectionName(ConnectionIsolationStrategy isolationStrategy, String defaultConnectionName) {
    if (isolationStrategy == null) {
      return defaultConnectionName;
    }

    return isolationStrategy.accept(new ConnectionIsolationStrategyVisitor<>() {

      @Override
      public String visit(SharedConnectionStrategy strategy) {
        return strategy.getName();
      }

      @Override
      public String visit(DefaultConnectionStrategy strategy) {
        return defaultConnectionName;
      }

    });
  }

}
