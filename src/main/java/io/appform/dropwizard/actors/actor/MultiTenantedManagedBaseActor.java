package io.appform.dropwizard.actors.actor;

import io.dropwizard.lifecycle.Managed;
import java.util.Set;

public class MultiTenantedManagedBaseActor<MessageType extends Enum<MessageType>, Message>
    extends MultiTenantedUnmanagedBaseActor<MessageType, Message> implements Managed {

  protected MultiTenantedManagedBaseActor(MessageType messageType, Class<? extends Message> clazz,
      Set<Class<?>> droppedExceptionTypes) {
    super(messageType, clazz, droppedExceptionTypes);
  }

  @Override
  public void start() throws Exception {

  }

  @Override
  public void stop() throws Exception {

  }
}
