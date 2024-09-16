package io.appform.dropwizard.actors.healthcheck;

import com.codahale.metrics.health.HealthCheck;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TenantAwareHealthCheck extends HealthCheck {

  private final Map<String, List<HealthCheck>> healthCheckMap = new ConcurrentHashMap<>();

  public void addHealthCheckForTenant(String tenantId, HealthCheck healthCheck) {
    Objects.requireNonNull(healthCheckMap.putIfAbsent(tenantId, new ArrayList<>())).add(healthCheck);
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
    return Result.unhealthy("All connections for all tenants are unhealthy!");
  }
}
