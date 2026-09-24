package org.example.ptit_cntt1_it214_session18_mini;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@EnableDiscoveryClient
@EnableFeignClients
public class PtitCntt1It214Session18MiniApplication {

    public static void main(String[] args) {
        SpringApplication.run(PtitCntt1It214Session18MiniApplication.class, args);
    }

}

