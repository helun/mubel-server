package io.mubel.server.mubelserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication
@EnableCaching
public class MubelServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(MubelServerApplication.class, args);
    }

}
