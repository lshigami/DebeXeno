package org.example.debexeno;

import java.util.HashSet;
import java.util.Set;
import org.example.debexeno.service.CaptureService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class DebeXenoApplication {

  public static void main(String[] args) {
    ApplicationContext context = SpringApplication.run(DebeXenoApplication.class, args);

    CaptureService captureService = context.getBean(CaptureService.class);

    Set<String> trackedTables = new HashSet<>();
    trackedTables.add("public.test_table");

    // Start the capture service
    captureService.startCapture(trackedTables);

    // Register shutdown hook to gracefully shut down the service.
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      captureService.stopCapture();
    }));
  }


}
