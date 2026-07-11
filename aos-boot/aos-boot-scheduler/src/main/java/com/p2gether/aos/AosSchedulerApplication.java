package com.p2gether.aos;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * The periodic-caller service (listener {@code SCH}): fires scheduled outbound calls
 * to other systems. FT (active/standby) mode is on by default in this module's
 * configuration so exactly one instance fires the schedule; standbys take over
 * automatically. Note that Spring's scheduler itself runs on every instance — each
 * {@code @Scheduled} job must start with a {@code subscriber.isActive()} guard (see
 * {@code SampleScheduledCall}).
 */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class AosSchedulerApplication {

    public static void main(String[] args) {
        SpringApplication.run(AosSchedulerApplication.class, args);
    }
}
