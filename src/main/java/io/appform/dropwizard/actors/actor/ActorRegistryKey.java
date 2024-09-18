package io.appform.dropwizard.actors.actor;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@EqualsAndHashCode
public class ActorRegistryKey {
  private final String tenantId;
  private final String actorKey;
}