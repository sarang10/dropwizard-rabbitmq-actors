package io.appform.dropwizard.actors;

import static io.appform.dropwizard.actors.utils.CommonUtils.UNKNOWN_TENANT;

import com.google.common.base.Preconditions;
import com.rabbitmq.client.Channel;
import io.appform.dropwizard.actors.common.RabbitmqActorException;
import io.appform.dropwizard.actors.config.RMQConfig;
import io.appform.dropwizard.actors.connectivity.RMQConnection;
import io.appform.dropwizard.actors.healthcheck.ClusterAwareHealthCheck;
import io.appform.dropwizard.actors.healthcheck.ConnectionHealthCheck;
import io.appform.dropwizard.actors.observers.RMQObserver;
import io.appform.dropwizard.actors.utils.CommonUtils;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Environment;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
@Data
public class MultiTenantedConnectionRegistry implements Managed {

  // Map of ClusterId, RMQConnectionMap
  // TODO : Do we need both inner and outer map as concurrent ?
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, RMQConnection>> connections;
  private final Environment environment;
  private final ExecutorServiceProvider executorServiceProvider;
  private final Map<String, RMQConfig> multiClusterRmqConfig;
  private final Map<String, TtlConfig> multiTenantedTtlConfig;
  private final Map<String, RMQObserver> multiTenantedRootObserver;
  private final ClusterAwareHealthCheck clusterAwareHealthCheck;

  public MultiTenantedConnectionRegistry(final Environment environment,
      final ExecutorServiceProvider executorServiceProvider,
      final Map<String, RMQConfig> multiClusterRmqConfig,
      final Map<String, TtlConfig> multiTenantedTtlConfig,
      final Map<String, RMQObserver> multiTenantedRootObserver,
      final ClusterAwareHealthCheck clusterAwareHealthCheck) {
    this.environment = environment;
    this.executorServiceProvider = executorServiceProvider;
    this.multiClusterRmqConfig = multiClusterRmqConfig;
    this.multiTenantedTtlConfig = multiTenantedTtlConfig;
    this.connections = new ConcurrentHashMap<>();
    this.multiTenantedRootObserver = multiTenantedRootObserver;
    this.clusterAwareHealthCheck = clusterAwareHealthCheck;
  }

  public RMQConnection createOrGet(String tenantId, String connectionName) {
    Preconditions.checkArgument(
        multiClusterRmqConfig.containsKey(tenantId), UNKNOWN_TENANT + tenantId);
    val threadPoolSize = determineThreadPoolSize(tenantId, connectionName);
    return createOrGet(tenantId, connectionName, threadPoolSize);
  }

  public RMQConnection createOrGet(String tenantId, String connectionName, int threadPoolSize) {

    Preconditions.checkArgument(
        multiClusterRmqConfig.containsKey(tenantId) && multiTenantedRootObserver.containsKey(tenantId)
        , UNKNOWN_TENANT + tenantId);

    return connections.computeIfAbsent(tenantId, k -> new ConcurrentHashMap<>())
        .computeIfAbsent(connectionName, connection -> {
            log.info(String.format("Creating new RMQ connection with name [%s] having [%d] threads", connection,
                threadPoolSize));
            val rmqConnection = new RMQConnection(
                connection,
                multiClusterRmqConfig.get(tenantId),
                executorServiceProvider.newFixedThreadPool(String.format("rmqconnection-%s", connection),
                    threadPoolSize),
                environment, multiTenantedTtlConfig.getOrDefault(tenantId, TtlConfig.builder()
                .build()), multiTenantedRootObserver.get(tenantId));
            Channel channel = rmqConnection.channel();
          clusterAwareHealthCheck.addHealthCheckForCluster(tenantId,
              new ConnectionHealthCheck(channel.getConnection(), channel));
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
        multiClusterRmqConfig.containsKey(tenantId), UNKNOWN_TENANT + tenantId);
    RMQConfig rmqConfig = multiClusterRmqConfig.get(tenantId);
    return CommonUtils.determineThreadPoolSize(connectionName, rmqConfig);
  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {
    connections.forEach((cluster, connectionMap) ->
        connectionMap.forEach(new BiConsumer<String, RMQConnection>() {
        @SneakyThrows
        @Override
        public void accept(String name, RMQConnection rmqConnection) {
          rmqConnection.stop();
        }
      }));
  }

}
