package io.appform.dropwizard.actors.actor;

import com.google.inject.Injector;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import lombok.val;

public class ActorRegistry {

  private final Map<ActorRegistryKey, MultiTenantedActor> multiTenantedActorMap = new HashMap<>();

  public ActorRegistry(Supplier<Injector> injector, Map<String, Map<String, ActorConfig>> multiTenantedActorConfig) {
    multiTenantedActorConfig.forEach((tenantId, actorMap) -> {
      actorMap.forEach(((actorKey, actorConfig) -> {
        MultiTenantedUnmanagedBaseActor multiTenantedUnmanagedbaseactor =
            null;
        try {
          multiTenantedUnmanagedbaseactor = (MultiTenantedUnmanagedBaseActor) injector.get()
              .getInstance(Class.forName(actorConfig.getClassPath()));
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
        MultiTenantedActor multiTenantedActor = new MultiTenantedActor(tenantId, actorConfig,
            multiTenantedUnmanagedbaseactor);
        multiTenantedActorMap.put(new ActorRegistryKey(tenantId, actorKey), multiTenantedActor);
      }));
    });
  }

  public MultiTenantedActor getTenantedActor(ActorRegistryKey actorRegistryKey) {
    return multiTenantedActorMap.get(actorRegistryKey);
  }
}
