package io.appform.dropwizard.actors.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.dropwizard.actors.MultiTenantedConnectionRegistry;
import io.appform.dropwizard.actors.exceptionhandler.ExceptionHandlingFactory;
import io.appform.dropwizard.actors.retry.RetryStrategyFactory;
import lombok.Getter;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j
public class RMQBundleDataProvider {

  @Getter
  private ObjectMapper objectMapper;
  @Getter
  private RetryStrategyFactory retryStrategyFactory;
  @Getter
  private ExceptionHandlingFactory exceptionHandlingFactory;
  @Getter
  private MultiTenantedConnectionRegistry multiTenantedConnectionRegistry;

  public void init(ObjectMapper objectMapper, RetryStrategyFactory retryStrategyFactory,
      ExceptionHandlingFactory exceptionHandlingFactory, MultiTenantedConnectionRegistry multiTenantedConnectionRegistry) {
    RMQBundleDataProvider.objectMapper = objectMapper;
    RMQBundleDataProvider.retryStrategyFactory = retryStrategyFactory;
    RMQBundleDataProvider.exceptionHandlingFactory = exceptionHandlingFactory;
    RMQBundleDataProvider.multiTenantedConnectionRegistry = multiTenantedConnectionRegistry;
  }
}