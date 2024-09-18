package io.appform.dropwizard.actors.actor;

import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Data
@EqualsAndHashCode
@ToString
@Slf4j
public abstract class MultiTenantedUnmanagedBaseActor<MessageType extends Enum<MessageType>, Message> {

  private MessageType type;
  private final Set<Class<?>> droppedExceptionTypes;
  Class<? extends Message> clazz;

  protected MultiTenantedUnmanagedBaseActor(MessageType type,
      Class<? extends Message> clazz, Set<Class<?>> droppedExceptionTypes) {
    this.type = type;
    this.clazz = clazz;
    this.droppedExceptionTypes = droppedExceptionTypes;
  }

  /*
    Override this method in your code for custom implementation.
 */
  protected boolean handle(Message message, MessageMetadata messageMetadata) throws Exception {
    return handle(message);
  }

  /*
      Override this method in your code in case you want to handle the expired messages separately
   */
  protected boolean handleExpiredMessages(Message message, MessageMetadata messageMetadata) throws Exception {
    return true;
  }

  @Deprecated
  protected boolean handle(Message message) throws Exception {
    throw new UnsupportedOperationException("Either implement this method, or implement the handle(message, messageMetadata) method");
  }

  public void start() throws Exception {

  }

  public void stop() throws Exception {

  }

}
