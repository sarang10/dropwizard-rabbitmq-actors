package io.appform.dropwizard.actors.healthcheck;

import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistryListener;
import io.dropwizard.setup.Environment;

public class ConnectionHealthCheckListener implements HealthCheckRegistryListener {

  private final Environment environment;

  public ConnectionHealthCheckListener(Environment environment) {
    this.environment = environment;
  }

  @Override
  public void onHealthCheckAdded(String s, HealthCheck healthCheck) {
    if(!(healthCheck instanceof ConnectionHealthCheck))
      return ;
    environment.healthChecks().unregister(s);
  }

  @Override
  public void onHealthCheckRemoved(String s, HealthCheck healthCheck) {

  }
}
