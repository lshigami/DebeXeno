package org.example.debexeno.offset;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.example.debexeno.coordination.DistributedCoordinationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

@ExtendWith(MockitoExtension.class)
class OffsetManagerTest {

  @TempDir
  Path tempDir;

  @Spy
  private ObjectMapper objectMapper;

  @Mock
  private DistributedCoordinationService coordinationService;

  @InjectMocks
  private OffsetManager offsetManager;

  @Mock
  private InterProcessMutex lock;

  private String offsetFilePath;

  @BeforeEach
  void setUp() {
    offsetFilePath = tempDir.resolve("test-offsets.json").toString();
    ReflectionTestUtils.setField(offsetManager, "offsetFilePath", offsetFilePath);
    ReflectionTestUtils.setField(offsetManager, "lockTimeoutMs", 1000L);
    ReflectionTestUtils.setField(offsetManager, "mapper", objectMapper);
  }


  @Test
  void loadOffsets_WhenFileExists_ShouldLoadOffsetsFromFile() throws IOException {
    //Given
    Map<String, String> mockOffsets = new HashMap<>();
    mockOffsets.put("slot1", "lsn1");
    mockOffsets.put("slot2", "lsn2");

    objectMapper.writeValue(new File(offsetFilePath), mockOffsets);
    // Setup stubs needed for this test
    when(coordinationService.acquireLock(anyString(), anyLong())).thenReturn(lock);
    doNothing().when(coordinationService).releaseLock(any(InterProcessMutex.class), anyString());
    //When

    offsetManager.loadOffsets();

    //Then

    assertEquals("lsn1", offsetManager.getOffset("slot1"));
    assertEquals("lsn2", offsetManager.getOffset("slot2"));

  }

  @Test
  void loadOffsets_WhenFileDoesNotExist_ShouldStartEmpty() {
    //Given

    // Setup stubs needed for this test
    when(coordinationService.acquireLock(anyString(), anyLong())).thenReturn(lock);
    doNothing().when(coordinationService).releaseLock(any(InterProcessMutex.class), anyString());
    //When
    offsetManager.loadOffsets();

    //Then
    assertNull(offsetManager.getOffset("slot1"));
  }

  @Test
  void saveOffsets_WhenOffsetsExist_ShouldSaveToFile() throws IOException {
    // Given
    ReflectionTestUtils.setField(offsetManager, "offsets",
        Map.of("slot1", "lsn1", "slot2", "lsn2"));

    // Setup stubs needed for this test
    when(coordinationService.acquireLock(anyString(), anyLong())).thenReturn(lock);
    doNothing().when(coordinationService).releaseLock(any(InterProcessMutex.class), anyString());

    // When
    offsetManager.saveOffsets();

    //Then
    File savedFile = new File(offsetFilePath);
    assertTrue(savedFile.exists());

    Map<String, String> loadedOffsets = objectMapper.readValue(savedFile, HashMap.class);
    assertEquals(2, loadedOffsets.size());
    assertEquals("lsn1", loadedOffsets.get("slot1"));
    assertEquals("lsn2", loadedOffsets.get("slot2"));
  }

  @Test
  void saveOffsets_WhenOffsetsEmpty_ShouldNotSaveFile() {
    //Given
    //When
    offsetManager.saveOffsets();
    //Then
    File savedFile = new File(offsetFilePath);
    assertFalse(savedFile.exists());
  }
}