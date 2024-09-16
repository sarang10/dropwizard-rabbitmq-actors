package io.appform.dropwizard.actors.healthcheck;

import com.codahale.metrics.health.HealthCheck;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClusterAwareHealthCheck extends HealthCheck {

  private final Map<String, List<HealthCheck>> healthCheckMap = new ConcurrentHashMap<>();

  public void addHealthCheckForCluster(String clusterId, HealthCheck healthCheck) {
    healthCheckMap.computeIfAbsent(clusterId, k -> new ArrayList<>()).add(healthCheck);
  }

  @Override
  protected Result check() throws Exception {

    for (List<HealthCheck> healthCheckList : healthCheckMap.values()) {
      AtomicBoolean healthy = new AtomicBoolean(true);
      for (HealthCheck healthCheck : healthCheckList) {
        if (!healthCheck.execute().isHealthy()) {
          healthy.set(false);
        }
      }
      if(healthy.get())
        return Result.healthy();
    }
    return Result.unhealthy("All connections for all clusters are unhealthy!");
  }
}
