package org.example.debexeno;

import org.example.debexeno.service.CaptureService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class DebeXenoApplication {

  public static void main(String[] args) {
    ApplicationContext context = SpringApplication.run(DebeXenoApplication.class, args);

    CaptureService captureService = context.getBean(CaptureService.class);
    captureService.captureChanges();
  }


}
