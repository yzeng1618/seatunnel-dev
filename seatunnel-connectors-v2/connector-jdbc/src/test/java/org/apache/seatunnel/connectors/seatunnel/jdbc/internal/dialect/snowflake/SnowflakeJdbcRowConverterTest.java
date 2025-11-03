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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class SnowflakeJdbcRowConverterTest {

    private SnowflakeJdbcRowConverter converter = new SnowflakeJdbcRowConverter();

    @Test
    public void testGetSnowflakeOffsetDateTimeWithOffsetDateTime() {
        OffsetDateTime expectedDateTime =
                OffsetDateTime.of(2023, 12, 25, 10, 30, 45, 123456000, ZoneOffset.ofHours(8));

        OffsetDateTime result = converter.getSnowflakeOffsetDateTime(expectedDateTime);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(expectedDateTime, result);
    }

    @Test
    public void testGetSnowflakeOffsetDateTimeWithZonedDateTime() {
        ZonedDateTime zonedDateTime =
                ZonedDateTime.of(2023, 12, 25, 10, 30, 45, 123456000, ZoneId.of("Asia/Shanghai"));
        OffsetDateTime expectedDateTime = zonedDateTime.toOffsetDateTime();

        OffsetDateTime result = converter.getSnowflakeOffsetDateTime(zonedDateTime);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(expectedDateTime, result);
    }

    @Test
    public void testGetSnowflakeOffsetDateTimeWithInstant() {
        Instant instant = Instant.parse("2023-12-25T10:30:45.123456Z");

        OffsetDateTime result = converter.getSnowflakeOffsetDateTime(instant);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(instant.atOffset(ZoneOffset.UTC), result);
    }

    @Test
    public void testGetSnowflakeOffsetDateTimeWithTimestamp() {
        java.sql.Timestamp sqlTimestamp = java.sql.Timestamp.valueOf("2023-12-25 10:30:45.123456");

        OffsetDateTime result = converter.getSnowflakeOffsetDateTime(sqlTimestamp);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(ZoneOffset.UTC, result.getOffset());
    }

    @Test
    public void testGetSnowflakeOffsetDateTimeWithDate() {
        java.util.Date utilDate = new java.util.Date(1703502645123L);

        OffsetDateTime result = converter.getSnowflakeOffsetDateTime(utilDate);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(ZoneOffset.UTC, result.getOffset());
    }

    @Test
    public void testGetSnowflakeOffsetDateTimeWithLong() {
        long epochMilli = 1703502645123L;

        OffsetDateTime result = converter.getSnowflakeOffsetDateTime(epochMilli);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(ZoneOffset.UTC, result.getOffset());
    }

    @Test
    public void testGetSnowflakeOffsetDateTimeWithString() {
        String dateTimeString = "2023-12-25T10:30:45.123456+08:00";

        OffsetDateTime result = converter.getSnowflakeOffsetDateTime(dateTimeString);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(ZoneOffset.ofHours(8), result.getOffset());
    }

    @Test
    public void testGetSnowflakeOffsetDateTimeWithNullValue() {
        OffsetDateTime result = converter.getSnowflakeOffsetDateTime(null);

        Assertions.assertNull(result);
    }

    @Test
    public void testConverterName() {
        Assertions.assertEquals("Snowflake", converter.converterName());
    }
}
