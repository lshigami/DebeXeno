package org.example.debexeno.offset;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class OffsetManager {

  @Value("${offset.storage.file:./offsets.json}")
  String offsetFilePath;

  Logger logger = LoggerFactory.getLogger(OffsetManager.class);

  private final ObjectMapper mapper = new ObjectMapper();

  private final Map<String, String> offsets = new ConcurrentHashMap<>();

  @PostConstruct
  public void init() {
    loadOffsets();
  }

  /**
   * Load offsets from storage file
   */
  public void loadOffsets() {
    logger.info("Loading offsets from file: {}", offsetFilePath);
    Path path = Path.of(offsetFilePath);
    if (Files.exists(path)) {
      try {
        Map<String, String> loadedOffsets = mapper.readValue(path.toFile(), HashMap.class);
        logger.info("Loaded offsets from {}: {}", offsetFilePath, offsets);
        offsets.putAll(loadedOffsets);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      logger.info("No offsets file {} found. Starting from scratch", offsetFilePath);
    }
  }

  /**
   * Save offsets to storage file
   */
  public void saveOffsets() {
    if (offsets.isEmpty()) {
      logger.info("No offsets to save");
      return;
    }
    File file = new File(offsetFilePath);
    file.getParentFile().mkdirs();
    try {
      mapper.writeValue(file, offsets);
      logger.info("Saved offsets to {}", file.getAbsolutePath());
    } catch (IOException e) {
      logger.error("Could not save offsets to {}", file.getAbsolutePath(), e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the lastest stred LSN for a given slot
   *
   * @param slotName Name of the replication slot
   * @return The last LSN or null if not found
   */
  public String getOffset(String slotName) {
    return offsets.get(slotName);
  }

  /**
   * Update the stored LSN for a given slot
   *
   * @param slotName Name of the replication slot
   * @param lsn      The new LSN to store
   */
  public void updateOffset(String slotName, String lsn) {
    if (lsn != null && !lsn.equals(getOffset(slotName))) {
      offsets.put(slotName, lsn);
      saveOffsets();
    }
  }


}
