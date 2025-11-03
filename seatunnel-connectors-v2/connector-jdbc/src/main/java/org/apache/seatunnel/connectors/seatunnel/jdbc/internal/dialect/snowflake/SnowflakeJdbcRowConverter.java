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

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcFieldTypeUtils;

import javax.annotation.Nullable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;

public class SnowflakeJdbcRowConverter extends AbstractJdbcRowConverter {
    @Override
    public String converterName() {
        return DatabaseIdentifier.SNOWFLAKE;
    }

    @Override
    public SeaTunnelRow toInternal(ResultSet rs, TableSchema tableSchema) throws SQLException {
        SeaTunnelRow row = super.toInternal(rs, tableSchema);
        // Handle TIMESTAMP_TZ types for Snowflake
        SeaTunnelRowType rowType = tableSchema.toPhysicalRowDataType();
        for (int fieldIndex = 0; fieldIndex < rowType.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = rowType.getFieldType(fieldIndex);
            if (seaTunnelDataType.getSqlType().equals(SqlType.TIMESTAMP_TZ)) {
                int resultSetIndex = fieldIndex + 1;
                OffsetDateTime offsetDateTime = getSnowflakeOffsetDateTime(rs, resultSetIndex);
                row.setField(fieldIndex, offsetDateTime);
            }
        }
        return row;
    }

    /**
     * Get OffsetDateTime from Snowflake TIMESTAMP_TZ or TIMESTAMP_LTZ column. Snowflake stores
     * these as OffsetDateTime or similar objects.
     */
    private OffsetDateTime getSnowflakeOffsetDateTime(ResultSet rs, int columnIndex)
            throws SQLException {
        Object obj = rs.getObject(columnIndex);
        return convertToOffsetDateTime(obj);
    }

    /** Convert an object to OffsetDateTime. This method is public for testing purposes. */
    public OffsetDateTime getSnowflakeOffsetDateTime(Object obj) {
        return convertToOffsetDateTime(obj);
    }

    /** Internal method to convert various types to OffsetDateTime. */
    private static OffsetDateTime convertToOffsetDateTime(Object obj) {
        if (obj == null) {
            return null;
        }

        // Handle OffsetDateTime directly
        if (obj instanceof OffsetDateTime) {
            return (OffsetDateTime) obj;
        }

        // Handle other time types
        if (obj instanceof java.time.ZonedDateTime) {
            return ((java.time.ZonedDateTime) obj).toOffsetDateTime();
        }

        if (obj instanceof java.time.Instant) {
            return ((java.time.Instant) obj).atOffset(java.time.ZoneOffset.UTC);
        }

        if (obj instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) obj).toLocalDateTime().atOffset(java.time.ZoneOffset.UTC);
        }

        if (obj instanceof java.util.Date) {
            return ((java.util.Date) obj).toInstant().atOffset(java.time.ZoneOffset.UTC);
        }

        if (obj instanceof Long) {
            return java.time.Instant.ofEpochMilli((Long) obj).atOffset(java.time.ZoneOffset.UTC);
        }

        // Try to parse as string
        String str = obj.toString();
        try {
            return JdbcFieldTypeUtils.parseOffsetDateTimeFromString(str);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse Snowflake TIMESTAMP_TZ value: " + str, e);
        }
    }

    @Override
    protected void setValueToStatementByDataType(
            Object value,
            PreparedStatement statement,
            SeaTunnelDataType<?> seaTunnelDataType,
            int statementIndex,
            @Nullable String sourceType)
            throws SQLException {
        if (seaTunnelDataType.getSqlType().equals(SqlType.TIMESTAMP_TZ)) {
            OffsetDateTime offsetDateTime = (OffsetDateTime) value;
            try {
                // Try to use setObject first for better timezone support
                statement.setObject(statementIndex, offsetDateTime);
            } catch (SQLException e) {
                // Fallback to setTimestamp if setObject is not supported
                statement.setTimestamp(
                        statementIndex, java.sql.Timestamp.from(offsetDateTime.toInstant()));
            }
        } else {
            super.setValueToStatementByDataType(
                    value, statement, seaTunnelDataType, statementIndex, sourceType);
        }
    }
}
