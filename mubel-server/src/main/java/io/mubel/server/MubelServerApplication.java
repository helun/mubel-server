package io.mubel.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;
import org.springframework.boot.autoconfigure.couchbase.CouchbaseAutoConfiguration;
import org.springframework.boot.autoconfigure.graphql.GraphQlAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jms.JmsAutoConfiguration;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication(
        scanBasePackages = {"io.mubel"},
        exclude = {
                DataSourceAutoConfiguration.class,
                JmsAutoConfiguration.class,
                GraphQlAutoConfiguration.class,
                CouchbaseAutoConfiguration.class,
                BatchAutoConfiguration.class
        })
@EnableCaching
public class MubelServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(MubelServerApplication.class, args);
    }

}
