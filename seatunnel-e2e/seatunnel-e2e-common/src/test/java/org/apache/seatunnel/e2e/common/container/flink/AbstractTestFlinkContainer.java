/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.common.container.flink;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.e2e.common.container.AbstractTestContainer;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * This class is the base class of FlinkEnvironment test. The before method will create a Flink
 * cluster, and after method will close the Flink cluster. You can use {@link
 * TestContainer#executeJob} to submit a seatunnel config and run a seatunnel job.
 */
@NoArgsConstructor
@Slf4j
public abstract class AbstractTestFlinkContainer extends AbstractTestContainer {

    protected static final List<String> DEFAULT_FLINK_PROPERTIES =
            Arrays.asList(
                    "jobmanager.rpc.address: jobmanager",
                    "taskmanager.numberOfTaskSlots: 10",
                    "parallelism.default: 4",
                    "env.java.opts: -Doracle.jdbc.timezoneAsRegion=false");

    protected static final String DEFAULT_DOCKER_IMAGE = "flink:1.13.6-scala_2.11";

    protected static final String MOUNTS_PATH = "/opt/seatunnel_mounts";

    protected GenericContainer<?> jobManager;
    protected GenericContainer<?> taskManager;

    @Override
    protected String getDockerImage() {
        return DEFAULT_DOCKER_IMAGE;
    }

    @Override
    public void startUp() throws Exception {
        final String dockerImage = getDockerImage();
        final String properties = String.join("\n", getFlinkProperties());

        // Debug logging for container startup
        System.out.println("=== AbstractTestFlinkContainer Startup Debug ===");
        System.out.println("Docker Image: " + dockerImage);
        System.out.println("FLINK_PROPERTIES environment variable (as passed to container):");
        System.out.println("--- START ENV VAR ---");
        System.out.println(properties);
        System.out.println("--- END ENV VAR ---");
        System.out.println("Properties length: " + properties.length() + " characters");
        System.out.println("Properties lines count: " + properties.split("\n").length);
        System.out.println("=== End Startup Debug ===");

        jobManager =
                new GenericContainer<>(dockerImage)
                        .withCommand("jobmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases("jobmanager")
                        .withExposedPorts()
                        .withEnv("FLINK_PROPERTIES", properties)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(dockerImage + ":jobmanager")))
                        .waitingFor(
                                new LogMessageWaitStrategy()
                                        .withRegEx(".*Starting the resource manager.*")
                                        .withStartupTimeout(Duration.ofMinutes(2)))
                        .withFileSystemBind(MOUNTS_PATH, MOUNTS_PATH, BindMode.READ_WRITE);
        copySeaTunnelStarterToContainer(jobManager);
        copySeaTunnelStarterLoggingToContainer(jobManager);

        jobManager.setPortBindings(Lists.newArrayList(String.format("%s:%s", 8081, 8081)));

        taskManager =
                new GenericContainer<>(dockerImage)
                        .withCommand("taskmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases("taskmanager")
                        .withEnv("FLINK_PROPERTIES", properties)
                        .dependsOn(jobManager)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                dockerImage + ":taskmanager")))
                        .waitingFor(
                                new LogMessageWaitStrategy()
                                        .withRegEx(
                                                ".*Successful registration at resource manager.*")
                                        .withStartupTimeout(Duration.ofMinutes(2)))
                        .withFileSystemBind(MOUNTS_PATH, MOUNTS_PATH, BindMode.READ_WRITE);

        Startables.deepStart(Stream.of(jobManager)).join();

        // Debug: Check container configuration after startup
        debugContainerConfiguration(jobManager);

        Startables.deepStart(Stream.of(taskManager)).join();
        // execute extra commands
        executeExtraCommands(jobManager);
    }

    protected List<String> getFlinkProperties() {
        return DEFAULT_FLINK_PROPERTIES;
    }

    /**
     * Debug method to inspect container configuration files after startup
     */
    protected void debugContainerConfiguration(GenericContainer<?> container) {
        try {
            System.out.println("=== Container Configuration Debug ===");

            // Check if container is running
            if (!container.isRunning()) {
                System.out.println("WARNING: Container is not running, cannot inspect configuration");
                return;
            }

            // List configuration directory contents
            System.out.println("Configuration directory contents:");
            try {
                Container.ExecResult result = container.execInContainer("ls", "-la", "/opt/flink/conf/");
                System.out.println("ls -la /opt/flink/conf/:");
                System.out.println(result.getStdout());
                if (!result.getStderr().isEmpty()) {
                    System.out.println("stderr: " + result.getStderr());
                }
            } catch (Exception e) {
                System.out.println("Failed to list /opt/flink/conf/: " + e.getMessage());
            }

            // Check for different possible config file names
            String[] configFiles = {"flink-conf.yaml", "config.yaml", "flink-config.yaml"};
            for (String configFile : configFiles) {
                try {
                    Container.ExecResult result = container.execInContainer("cat", "/opt/flink/conf/" + configFile);
                    if (result.getExitCode() == 0) {
                        System.out.println("=== Content of /opt/flink/conf/" + configFile + " ===");
                        System.out.println(result.getStdout());
                        System.out.println("=== End of " + configFile + " ===");
                    }
                } catch (Exception e) {
                    System.out.println("File /opt/flink/conf/" + configFile + " not found or not readable");
                }
            }

            // Check environment variables
            try {
                Container.ExecResult result = container.execInContainer("env");
                System.out.println("=== Environment Variables ===");
                String[] envLines = result.getStdout().split("\n");
                for (String line : envLines) {
                    if (line.contains("FLINK") || line.contains("JAVA") || line.contains("JVM")) {
                        System.out.println(line);
                    }
                }
                System.out.println("=== End Environment Variables ===");
            } catch (Exception e) {
                System.out.println("Failed to get environment variables: " + e.getMessage());
            }

            System.out.println("=== End Container Configuration Debug ===");
        } catch (Exception e) {
            System.out.println("Error during container configuration debug: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void tearDown() throws Exception {
        if (taskManager != null) {
            taskManager.stop();
        }
        if (jobManager != null) {
            jobManager.stop();
        }
    }

    @Override
    protected String getSavePointCommand() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getCancelJobCommand() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getRestoreCommand() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected List<String> getExtraStartShellCommands() {
        return Collections.emptyList();
    }

    public void executeExtraCommands(ContainerExtendedFactory extendedFactory)
            throws IOException, InterruptedException {
        extendedFactory.extend(jobManager);
        extendedFactory.extend(taskManager);
    }

    @Override
    public Container.ExecResult executeJob(String confFile)
            throws IOException, InterruptedException {
        return executeJob(confFile, Collections.emptyList());
    }

    @Override
    public Container.ExecResult executeJob(String confFile, List<String> variables)
            throws IOException, InterruptedException {
        log.info("test in container: {}", identifier());
        return executeJob(jobManager, confFile, null, variables);
    }

    @Override
    public String getServerLogs() {
        return jobManager.getLogs() + "\n" + taskManager.getLogs();
    }

    public String executeJobManagerInnerCommand(String command)
            throws IOException, InterruptedException {
        return jobManager.execInContainer("bash", "-c", command).getStdout();
    }

    @Override
    public void copyFileToContainer(String path, String targetPath) {
        ContainerUtil.copyFileIntoContainers(
                ContainerUtil.getResourcesFile(path).toPath(), targetPath, jobManager);
    }

    @Override
    public void copyAbsolutePathToContainer(String path, String targetPath) {
        ContainerUtil.copyFileIntoContainers(Paths.get(path), targetPath, jobManager);
    }
}
