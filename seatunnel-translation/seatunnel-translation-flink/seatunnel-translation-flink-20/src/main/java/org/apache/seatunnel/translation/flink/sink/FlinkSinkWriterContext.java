/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.seatunnel.translation.flink.sink;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.DefaultEventProcessor;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.translation.flink.metric.FlinkMetricContext;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

@Slf4j
public class FlinkSinkWriterContext implements SinkWriter.Context {

    private final Sink.InitContext initContext;
    private final int parallelism;
    private final EventListener eventListener;

    public FlinkSinkWriterContext(Sink.InitContext initContext, int parallelism) {
        this.initContext = initContext;
        this.parallelism = parallelism;
        this.eventListener = new DefaultEventProcessor(getFlinkJobId(initContext));
        log.info("FlinkSinkWriterContext initialized with parallelism: {}", parallelism);
    }

    @Override
    public int getIndexOfSubtask() {
        return initContext.getSubtaskId();
    }

    @Override
    public int getNumberOfParallelSubtasks() {
        return parallelism;
    }

    @Override
    public MetricsContext getMetricsContext() {
        try {
            RuntimeContext runtimeContext = getRuntimeContext();
            MetricGroup metricGroup = initContext.metricGroup();

            if (runtimeContext != null && metricGroup != null) {
                // 使用支持accumulator的构造函数
                return new FlinkMetricContext(runtimeContext, metricGroup);
            } else {
                log.warn("RuntimeContext or MetricGroup is null, using fallback");
                return new FlinkMetricContext(metricGroup);
            }
        } catch (Exception e) {
            log.warn("Failed to create metrics context", e);
            // 返回一个空的MetricsContext而不是null，避免NPE
            return new FlinkMetricContext((MetricGroup) null);
        }
    }

    @Override
    public EventListener getEventListener() {
        return eventListener;
    }

    /**
     * 获取RuntimeContext，用于注册accumulator 在Flink 1.20中，InitContext可能有多种实现： 1.
     * InitContextImpl继承自InitContextBase 2. InitContextWrapper包装了实际的InitContext 我们需要处理这两种情况
     *
     * @return RuntimeContext实例，如果无法获取则返回null
     */
    public RuntimeContext getRuntimeContext() {
        try {
            log.debug(
                    "Attempting to get RuntimeContext from InitContext: {}",
                    initContext.getClass().getName());

            // 优先使用字段扫描方法，因为测试证明这是最可靠的
            RuntimeContext runtimeContext = tryGetFromFields(initContext);
            if (runtimeContext != null) {
                return runtimeContext;
            }

            // 备选方法1: 尝试直接从InitContextBase获取
            runtimeContext = tryGetFromInitContextBase(initContext);
            if (runtimeContext != null) {
                return runtimeContext;
            }

            // 备选方法2: 如果是包装器类，尝试获取被包装的对象
            runtimeContext = tryGetFromWrapper(initContext);
            if (runtimeContext != null) {
                return runtimeContext;
            }

            log.warn(
                    "Failed to obtain RuntimeContext from InitContext: {}",
                    initContext.getClass().getName());
            return null;

        } catch (Exception e) {
            log.warn("Failed to get RuntimeContext via reflection", e);
            return null;
        }
    }

    /** 尝试从InitContextBase获取RuntimeContext */
    private RuntimeContext tryGetFromInitContextBase(Object context) {
        try {
            Class<?> initContextBaseClass =
                    Class.forName(
                            "org.apache.flink.streaming.runtime.operators.sink.InitContextBase");
            if (initContextBaseClass.isInstance(context)) {
                Method getRuntimeContextMethod =
                        initContextBaseClass.getDeclaredMethod("getRuntimeContext");
                getRuntimeContextMethod.setAccessible(true);
                RuntimeContext runtimeContext =
                        (RuntimeContext) getRuntimeContextMethod.invoke(context);
                log.info(
                        "Successfully obtained RuntimeContext from InitContextBase: {}",
                        runtimeContext.getClass().getName());
                return runtimeContext;
            }
        } catch (Exception e) {
            log.debug("Failed to get RuntimeContext from InitContextBase", e);
        }
        return null;
    }

    /** 尝试从包装器类获取RuntimeContext */
    private RuntimeContext tryGetFromWrapper(Object context) {
        try {
            // 查找可能的包装字段名
            String[] possibleFieldNames = {
                "delegate", "wrapped", "context", "initContext", "writerInitContext"
            };

            Class<?> contextClass = context.getClass();
            for (String fieldName : possibleFieldNames) {
                try {
                    Field field = contextClass.getDeclaredField(fieldName);
                    field.setAccessible(true);
                    Object wrappedContext = field.get(context);

                    if (wrappedContext != null) {
                        log.debug(
                                "Found wrapped context in field '{}': {}",
                                fieldName,
                                wrappedContext.getClass().getName());

                        // 递归尝试从包装的对象获取RuntimeContext
                        RuntimeContext runtimeContext = tryGetFromInitContextBase(wrappedContext);
                        if (runtimeContext != null) {
                            log.info(
                                    "Successfully obtained RuntimeContext from wrapped context: {}",
                                    runtimeContext.getClass().getName());
                            return runtimeContext;
                        }
                    }
                } catch (NoSuchFieldException ignored) {
                    // 继续尝试下一个字段名
                }
            }
        } catch (Exception e) {
            log.debug("Failed to get RuntimeContext from wrapper", e);
        }
        return null;
    }

    /** 尝试通过反射查找RuntimeContext字段 */
    private RuntimeContext tryGetFromFields(Object context) {
        try {
            Class<?> contextClass = context.getClass();
            Field[] fields = contextClass.getDeclaredFields();

            for (Field field : fields) {
                if (RuntimeContext.class.isAssignableFrom(field.getType())
                        || StreamingRuntimeContext.class.isAssignableFrom(field.getType())) {

                    field.setAccessible(true);
                    RuntimeContext runtimeContext = (RuntimeContext) field.get(context);
                    if (runtimeContext != null) {
                        log.info(
                                "Successfully obtained RuntimeContext from field '{}': {}",
                                field.getName(),
                                runtimeContext.getClass().getName());
                        return runtimeContext;
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Failed to get RuntimeContext from fields", e);
        }
        return null;
    }

    private static String getFlinkJobId(Sink.InitContext context) {
        try {
            // 尝试获取JobID，如果无法获取则返回null
            return context.getJobInfo().getJobId().toString();
        } catch (Exception e) {
            log.warn("Get flink job id failed", e);
            return null;
        }
    }
}
