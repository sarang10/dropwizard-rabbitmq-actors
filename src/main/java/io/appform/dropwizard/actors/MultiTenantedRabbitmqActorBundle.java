package io.appform.dropwizard.actors;

import static io.appform.dropwizard.actors.utils.CommonUtils.UNKNOWN_TENANT;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import io.appform.dropwizard.actors.config.RMQConfig;
import io.appform.dropwizard.actors.healthcheck.ClusterAwareHealthCheck;
import io.appform.dropwizard.actors.healthcheck.ConnectionHealthCheckListener;
import io.appform.dropwizard.actors.metrics.RMQMetricObserver;
import io.appform.dropwizard.actors.observers.RMQObserver;
import io.appform.dropwizard.actors.observers.TerminalRMQObserver;
import io.dropwizard.Configuration;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
public abstract class MultiTenantedRabbitmqActorBundle<T extends Configuration> implements
    ConfiguredBundle<T> {

  @Getter
  private MultiTenantedConnectionRegistry connectionRegistry;
  private final Map<String, List<RMQObserver>> multiTenantedObservers = new ConcurrentHashMap<>();
  private final Map<String, RMQObserver> multiTenantedRootObserver = new HashMap<>();
  private Map<String, RMQConfig> multiClusterRmqConfig;

  protected MultiTenantedRabbitmqActorBundle() {}

  @Override
  public void initialize(Bootstrap<?> bootstrap) {}

  protected abstract Map<String, TtlConfig> getMultiTenantedTtlConfig(T t);
  protected abstract Map<String, RMQConfig> getMultiTenantRmqConfig(T t);

  protected ExecutorServiceProvider getExecutorServiceProvider(T t) {
    return (name, coreSize) -> Executors.newFixedThreadPool(coreSize);
  }

  @Override
  public void run(T t, Environment environment) {
    multiClusterRmqConfig = getMultiTenantRmqConfig(t);
    val executorServiceProvider = getExecutorServiceProvider(t);
    Preconditions.checkNotNull(executorServiceProvider, "Null executor service provider provided");

    val ttlConfig = getMultiTenantedTtlConfig(t) == null ?
        new HashMap<String, TtlConfig>() : getMultiTenantedTtlConfig(t);

    //TODO : Exception Handling ? Preconditions.checkNotNull(rootObserver, "Null root observer provided");
    multiClusterRmqConfig.forEach((tenantId, rmqConfig) ->
        multiTenantedRootObserver.put(tenantId, setupObservers(
            environment.metrics(), multiTenantedObservers.computeIfAbsent(tenantId, k -> new ArrayList<>()),
            rmqConfig, tenantId)));

    ClusterAwareHealthCheck clusterAwareHealthCheck = new ClusterAwareHealthCheck();
    this.connectionRegistry = new MultiTenantedConnectionRegistry(environment, executorServiceProvider,
        multiClusterRmqConfig, ttlConfig, multiTenantedRootObserver, clusterAwareHealthCheck);
    environment.lifecycle().manage(connectionRegistry);

    environment.healthChecks().addListener(new ConnectionHealthCheckListener(environment));
    environment.healthChecks().register("ClusterAwareRMQHealthCheck", clusterAwareHealthCheck);
  }

  public void registerObserverForTenant(final String tenantId, final RMQObserver observer) {
    if (null == observer) {
      return;
    }
    Preconditions.checkArgument(
        multiClusterRmqConfig.containsKey(tenantId), UNKNOWN_TENANT + tenantId);
    this.multiTenantedObservers.computeIfAbsent(tenantId, k -> new ArrayList<>()).add(observer);
    log.info("Registered observer: " + observer.getClass().getSimpleName() + " for tenant id: " + tenantId);
  }

  private RMQObserver setupObservers(final MetricRegistry metricRegistry,
      List<RMQObserver> observerList, RMQConfig rmqConfig, String tenantId) {
    //Terminal observer calls the actual method
    RMQObserver rootObserver = new TerminalRMQObserver();
    for (var observer : observerList) {
      if (null != observer) {
        rootObserver = observer.setNext(rootObserver);
      }
    }

    rootObserver = new RMQMetricObserver(rmqConfig, metricRegistry, true, tenantId).setNext(rootObserver);
    log.info("Root observer is {}", rootObserver.getClass().getSimpleName());
    return rootObserver;
  }

}
