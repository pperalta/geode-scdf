package org.springframework.cloud.dataflow.geode;

import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.dataflow.core.ModuleCoordinates;
import org.springframework.cloud.dataflow.core.ModuleDefinition;
import org.springframework.cloud.dataflow.core.ModuleDeploymentId;
import org.springframework.cloud.dataflow.core.ModuleDeploymentRequest;

@SpringBootApplication
public class GeodeApplication {

    private static final Logger logger = LoggerFactory.getLogger(GeodeApplication.class);

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        // locator must be launched externally
//        properties.put("start-locator", "localhost[7777]");
        properties.put("locators", "localhost[7777]");
        properties.put("log-level", "warning");
        Cache cache = new CacheFactory(properties).create();
        RegionFactory<ModuleDeploymentId, ModuleDeploymentRequest> factory =
                cache.createRegionFactory(RegionShortcut.REPLICATE);
        factory.addCacheListener(new ModuleDeploymentCacheListener());
        Region<ModuleDeploymentId, ModuleDeploymentRequest> region = factory.create("module-deployments");
        logger.info("Created region {}", region);

//        logger.info("launching modules in 10 seconds...");
//        Thread.sleep(10000);
//
//        launchModule(region, "tt", "log", "sink");
//        launchModule(region, "tt", "time", "source");

        SpringApplication.run(GeodeApplication.class, args);
    }

    private static void launchModule(Region<ModuleDeploymentId, ModuleDeploymentRequest> region,
            String group, String name, String type) {
        ModuleDefinition moduleDefinition = new ModuleDefinition.Builder()
                .setName(name)
                .setGroup(group)
                .setParameter("spring.cloud.stream.bindings.input", "output")
                .build();
        ModuleDeploymentRequest request = new ModuleDeploymentRequest(
                moduleDefinition,
                new ModuleCoordinates.Builder()
                        .setGroupId("org.springframework.cloud.stream.module")
                        .setArtifactId(String.format("%s-%s", name, type))
                        .setVersion("1.0.0.BUILD-SNAPSHOT")
                        .build());

        ModuleDeploymentId id = ModuleDeploymentId.fromModuleDefinition(moduleDefinition);

        region.put(id, request);
    }
}
