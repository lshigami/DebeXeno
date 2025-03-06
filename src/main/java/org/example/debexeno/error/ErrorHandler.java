package org.example.debexeno.error;

import java.util.function.Supplier;
import org.example.debexeno.reader.ChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ErrorHandler {

  private static final Logger logger = LoggerFactory.getLogger(ErrorHandler.class);

  @Autowired
  private DeadLetterQueue deadLetterQueue;

  @Value("${error.max.retries:3}")
  private int maxRetries;

  @Value("${error.retry.backoff.ms:1000}")
  private long retryBackoffMs;

  /**
   * Executes an operation with retry logic
   *
   * @param <T>       The return type of the operation
   * @param operation The operation to execute
   * @param event     The event being processed
   * @param phase     The processing phase
   * @return The result of the operation
   * @throws Exception If the operation fails after all retries
   */
  public <T> T executeWithRetry(Supplier<T> operation, ChangeEvent event, String phase) {
    deadLetterQueue.ensureTopicExists();
    int retries = 0;
    Exception lastException = null;
    while (retries <= maxRetries) {
      try {
        T result = operation.get();
        return result;
      } catch (Exception e) {
        retries++;
        lastException = e;
        logger.warn("Error during {} (retry {}/{}): {}", phase, retries, maxRetries,
            e.getMessage());
        if (retries <= maxRetries) {
          long backoff = retryBackoffMs * (long) Math.pow(2, retries - 1);
          logger.info("Retrying after {} ms", backoff);
          try {
            Thread.sleep(backoff);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Retry interrupted", ie);
          }
        }

      }
    }
    deadLetterQueue.sendToDeadLetterQueue(event, lastException, phase);
    throw new RuntimeException("Failed after " + maxRetries + " retries during " + phase,
        lastException);
  }

}
