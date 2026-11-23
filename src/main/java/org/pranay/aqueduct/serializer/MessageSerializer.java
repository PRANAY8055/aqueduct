package org.pranay.aqueduct.serializer;

import org.pranay.aqueduct.message.Message;
import org.pranay.aqueduct.exception.DeserializerException;

/**
 * Serializer serialize Message into string format.
 */
public interface MessageSerializer {

  /**
   * Serialize kafka message into string.
   *
   * @param message the message
   * @return serialised message
   * @throws DeserializerException the deserializer exception
   */
  String serialize(Message message) throws DeserializerException;
}
