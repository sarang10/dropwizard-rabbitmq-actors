package io.appform.dropwizard.actors;

import com.google.common.base.Preconditions;
import io.appform.dropwizard.actors.common.Constants;
import io.appform.dropwizard.actors.common.RabbitmqActorException;
import io.appform.dropwizard.actors.config.RMQConfig;
import io.appform.dropwizard.actors.connectivity.ConnectionConfig;
import io.appform.dropwizard.actors.connectivity.RMQConnection;
import io.appform.dropwizard.actors.healthcheck.ConnectionHealthCheck;
import io.appform.dropwizard.actors.healthcheck.TenantAwareHealthCheck;
import io.appform.dropwizard.actors.observers.RMQObserver;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Environment;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
@Data
public class MultiTenantedConnectionRegistry implements Managed {

  private final ConcurrentHashMap<String, ConcurrentHashMap<String, RMQConnection>> connections;
  private final Environment environment;
  private final ExecutorServiceProvider executorServiceProvider;
  private final Map<String, RMQConfig> multiTenantedRmqConfig;
  private final Map<String, String> tenantToClusterMapping;
  private final Map<String, TtlConfig> multiTenantedTtlConfig;
  private final Map<String, RMQObserver> multiTenantedRootObserver;
  private final TenantAwareHealthCheck tenantAwareHealthCheck;

  public MultiTenantedConnectionRegistry(final Environment environment,
      final ExecutorServiceProvider executorServiceProvider,
      final Map<String, RMQConfig> multiTenantedRmqConfig,
      final Map<String, TtlConfig> multiTenantedTtlConfig,
      final Map<String, String> tenantToClusterMapping,
      final Map<String, RMQObserver> multiTenantedRootObserver,
      final TenantAwareHealthCheck tenantAwareHealthCheck) {
    this.environment = environment;
    this.executorServiceProvider = executorServiceProvider;
    this.multiTenantedRmqConfig = multiTenantedRmqConfig;
    this.multiTenantedTtlConfig = multiTenantedTtlConfig;
    this.connections = new ConcurrentHashMap<>();
    this.tenantToClusterMapping = tenantToClusterMapping;
    this.multiTenantedRootObserver = multiTenantedRootObserver;
    this.tenantAwareHealthCheck = tenantAwareHealthCheck;
  }

  public RMQConnection createOrGet(String tenantId, String connectionName) {
    Preconditions.checkArgument(
        multiTenantedRmqConfig.containsKey(tenantId), "Unknown tenant: " + tenantId);
    val threadPoolSize = determineThreadPoolSize(tenantId, connectionName);
    return createOrGet(tenantId, connectionName, threadPoolSize);
  }

  public RMQConnection createOrGet(String tenantId, String connectionName, int threadPoolSize) {

    Preconditions.checkArgument(
        multiTenantedRmqConfig.containsKey(tenantId) && multiTenantedTtlConfig.containsKey(tenantId)
        && multiTenantedRootObserver.containsKey(tenantId)
        , "Unknown tenant: " + tenantId);

    return connections.computeIfAbsent(tenantId, id -> new ConcurrentHashMap<>())
        .computeIfAbsent(connectionName, connection -> {
            log.info(String.format("Creating new RMQ connection with name [%s] having [%d] threads", connection,
                threadPoolSize));
            val rmqConnection = new RMQConnection(
                connection,
                multiTenantedRmqConfig.get(tenantId),
                executorServiceProvider.newFixedThreadPool(String.format("rmqconnection-%s", connection),
                    threadPoolSize),
                environment, multiTenantedTtlConfig.get(tenantId), multiTenantedRootObserver.get(tenantId));
          tenantAwareHealthCheck.addHealthCheckForTenant(tenantId,
              new ConnectionHealthCheck(rmqConnection.channel().getConnection(), rmqConnection.channel()));
            try {
              rmqConnection.start();
            } catch (Exception e) {
              throw RabbitmqActorException.propagate(e);
            }
            log.info(String.format("Created new RMQ connection with name [%s]", connection));
            return rmqConnection;
      });
  }

  private int determineThreadPoolSize(String tenantId, String connectionName) {
    Preconditions.checkArgument(
        multiTenantedRmqConfig.containsKey(tenantId), "Unknown tenant: " + tenantId);
    RMQConfig rmqConfig = multiTenantedRmqConfig.get(tenantId);
    if (Constants.DEFAULT_CONNECTIONS.contains(connectionName)) {
      return rmqConfig.getThreadPoolSize();
    }

    if (rmqConfig.getConnections() == null) {
      return Constants.DEFAULT_THREADS_PER_CONNECTION;
    }

    return rmqConfig.getConnections().stream()
        .filter(x -> Objects.equals(x.getName(), connectionName))
        .findAny()
        .map(ConnectionConfig::getThreadPoolSize)
        .orElse(Constants.DEFAULT_THREADS_PER_CONNECTION);
  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {
    connections.forEach((tenant, connectionMap) ->
        connectionMap.forEach(new BiConsumer<String, RMQConnection>() {
        @SneakyThrows
        @Override
        public void accept(String name, RMQConnection rmqConnection) {
          rmqConnection.stop();
        }
      }));
  }

}
