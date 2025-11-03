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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.snowflake;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SnowflakeTypeConverterTest {

    private static final SnowflakeTypeConverter INSTANCE = SnowflakeTypeConverter.INSTANCE;

    @Test
    public void testConvertTimestampTz() {
        // Test TIMESTAMPTZ without scale
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test_tz")
                        .columnType("TIMESTAMPTZ")
                        .dataType("TIMESTAMPTZ")
                        .build();
        Column column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.OFFSET_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(9, column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        // Test TIMESTAMPTZ with scale
        typeDefine =
                BasicTypeDefine.builder()
                        .name("test_tz_scale")
                        .columnType("TIMESTAMPTZ(6)")
                        .dataType("TIMESTAMPTZ")
                        .scale(6)
                        .build();
        column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.OFFSET_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(9, column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertTimestampLtz() {
        // Test TIMESTAMPLTZ without scale
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test_ltz")
                        .columnType("TIMESTAMPLTZ")
                        .dataType("TIMESTAMPLTZ")
                        .build();
        Column column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.OFFSET_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(9, column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());

        // Test TIMESTAMPLTZ with scale
        typeDefine =
                BasicTypeDefine.builder()
                        .name("test_ltz_scale")
                        .columnType("TIMESTAMPLTZ(3)")
                        .dataType("TIMESTAMPLTZ")
                        .scale(3)
                        .build();
        column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.OFFSET_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(9, column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testReconvertTimestampTz() {
        // Test OFFSET_DATE_TIME_TYPE to TIMESTAMPTZ
        Column column =
                PhysicalColumn.builder()
                        .name("test_tz")
                        .dataType(LocalTimeType.OFFSET_DATE_TIME_TYPE)
                        .scale(6)
                        .build();

        BasicTypeDefine typeDefine = INSTANCE.reconvert(column);

        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("TIMESTAMPTZ", typeDefine.getColumnType());
        Assertions.assertEquals("TIMESTAMPTZ", typeDefine.getDataType());
        Assertions.assertEquals(6, typeDefine.getScale());
    }

    @Test
    public void testReconvertTimestampTzWithMaxScale() {
        // Test OFFSET_DATE_TIME_TYPE with scale > 9 (should be capped at 9)
        Column column =
                PhysicalColumn.builder()
                        .name("test_tz_max_scale")
                        .dataType(LocalTimeType.OFFSET_DATE_TIME_TYPE)
                        .scale(15)
                        .build();

        BasicTypeDefine typeDefine = INSTANCE.reconvert(column);

        Assertions.assertEquals(column.getName(), typeDefine.getName());
        Assertions.assertEquals("TIMESTAMPTZ", typeDefine.getColumnType());
        Assertions.assertEquals("TIMESTAMPTZ", typeDefine.getDataType());
        // Scale should be capped at 9
        Assertions.assertEquals(9, typeDefine.getScale());
    }

    @Test
    public void testConvertTimestamp() {
        // Test regular TIMESTAMP (without timezone)
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test_timestamp")
                        .columnType("TIMESTAMP")
                        .dataType("TIMESTAMP")
                        .build();
        Column column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(9, column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertTimestampNtz() {
        // Test TIMESTAMPNTZ (no timezone)
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test_ntz")
                        .columnType("TIMESTAMPNTZ")
                        .dataType("TIMESTAMPNTZ")
                        .build();
        Column column = INSTANCE.convert(typeDefine);

        Assertions.assertEquals(typeDefine.getName(), column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(9, column.getScale());
        Assertions.assertEquals(typeDefine.getColumnType(), column.getSourceType());
    }

    @Test
    public void testConvertUnsupported() {
        BasicTypeDefine<Object> typeDefine =
                BasicTypeDefine.builder()
                        .name("test")
                        .columnType("UNSUPPORTED_TYPE")
                        .dataType("UNSUPPORTED_TYPE")
                        .build();
        try {
            INSTANCE.convert(typeDefine);
            Assertions.fail("Should throw exception for unsupported type");
        } catch (SeaTunnelRuntimeException e) {
            // Expected
        }
    }

    @Test
    public void testReconvertUnsupported() {
        Column column =
                PhysicalColumn.builder().name("test").dataType(BasicType.STRING_TYPE).build();
        // This should work as STRING maps to VARCHAR
        BasicTypeDefine typeDefine = INSTANCE.reconvert(column);
        Assertions.assertNotNull(typeDefine);
    }
}
