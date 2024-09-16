package io.appform.dropwizard.actors;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import io.appform.dropwizard.actors.config.RMQConfig;
import io.appform.dropwizard.actors.healthcheck.ConnectionHealthCheckListener;
import io.appform.dropwizard.actors.healthcheck.TenantAwareHealthCheck;
import io.appform.dropwizard.actors.metrics.RMQMetricObserver;
import io.appform.dropwizard.actors.observers.RMQObserver;
import io.appform.dropwizard.actors.observers.TerminalRMQObserver;
import io.dropwizard.Configuration;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
  private Map<String, RMQConfig> multiClusterRmqConfig;
  private Map<String, String> tenantToClusterMapping;
  private final Map<String, List<RMQObserver>> multiTenantedObservers = new ConcurrentHashMap<>();
  private final Map<String, RMQObserver> multiTenantedRootObserver = new ConcurrentHashMap<>();

  protected MultiTenantedRabbitmqActorBundle() {}

  @Override
  public void initialize(Bootstrap<?> bootstrap) {
  }

  protected abstract Map<String, TtlConfig> getMultiTenantedTtlConfig(T t);
  protected abstract Map<String, String> getTenantToClusterMapping(T t);
  protected abstract Map<String, RMQConfig> getMultiClusterRmqConfig(T t);

  protected ExecutorServiceProvider getExecutorServiceProvider(T t) {
    return (name, coreSize) -> Executors.newFixedThreadPool(coreSize);
  }

  @Override
  public void run(T t, Environment environment) {
    this.multiClusterRmqConfig = getMultiClusterRmqConfig(t);
    this.tenantToClusterMapping = getTenantToClusterMapping(t);
    val executorServiceProvider = getExecutorServiceProvider(t);
    val ttlConfig = getMultiTenantedTtlConfig(t) == null ?
        new ConcurrentHashMap<String, TtlConfig>() : getMultiTenantedTtlConfig(t);
    Preconditions.checkNotNull(executorServiceProvider, "Null executor service provider provided");
    multiTenantedObservers.forEach((tenantId, rmqObserverList) ->
        multiTenantedRootObserver.put(tenantId,setupObserversForTenant(tenantId, environment.metrics())));
    this.connectionRegistry = new MultiTenantedConnectionRegistry(environment, executorServiceProvider,
        multiClusterRmqConfig, ttlConfig, tenantToClusterMapping, multiTenantedRootObserver, new TenantAwareHealthCheck());
    environment.lifecycle().manage(connectionRegistry);
    environment.healthChecks().addListener(new ConnectionHealthCheckListener(environment));

  }

  public void registerObserverForTenant(final String tenantId, final RMQObserver observer) {
    if (null == observer) {
      return;
    }
    Preconditions.checkArgument(
        tenantToClusterMapping.containsKey(tenantId), "Unknown tenant: " + tenantId);
    Objects.requireNonNull(this.multiTenantedObservers.putIfAbsent(tenantId, new ArrayList<>())).add(observer);
    log.info("Registered observer: " + observer.getClass().getSimpleName() + " for tenant id: " + tenantId);
  }

  private RMQObserver setupObserversForTenant(final String tenantId, final MetricRegistry metricRegistry) {
    Preconditions.checkArgument(
        multiTenantedObservers.containsKey(tenantId) && multiClusterRmqConfig.containsKey(tenantId)
        , "Unknown tenant: " + tenantId);
    //Terminal observer calls the actual method
    RMQObserver rootObserver = new TerminalRMQObserver();
    for (var observer : this.multiTenantedObservers.get(tenantId)) {
      if (null != observer) {
        rootObserver = observer.setNext(rootObserver);
      }
    }
    rootObserver = new RMQMetricObserver(this.multiClusterRmqConfig.get(tenantId), metricRegistry).setNext(rootObserver);
    log.info("Root observer is {}", rootObserver.getClass().getSimpleName());
    return rootObserver;
  }

}
