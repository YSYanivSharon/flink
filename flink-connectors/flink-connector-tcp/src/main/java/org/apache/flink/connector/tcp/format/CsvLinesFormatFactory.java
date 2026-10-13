/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.tcp.format;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.csv.CsvRowDataDeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Set;

/**
 * {@code 'format' = 'csv-lines'}: CSV where one message carries many newline-separated rows, as
 * NebulaStream's Kafka sink sends one message per buffer.
 */
public final class CsvLinesFormatFactory implements DeserializationFormatFactory {

    public static final ConfigOption<String> FIELD_DELIMITER =
            ConfigOptions.key("field-delimiter").stringType().defaultValue(",");

    @Override
    public String factoryIdentifier() {
        return "csv-lines";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of(FIELD_DELIMITER);
    }

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        final char delimiter = formatOptions.get(FIELD_DELIMITER).charAt(0);
        return new DecodingFormat<>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context sourceContext, DataType physicalDataType) {
                final TypeInformation<RowData> typeInfo =
                        sourceContext.createTypeInformation(physicalDataType);
                return new CsvLinesDeserializationSchema(
                        new CsvRowDataDeserializationSchema.Builder(
                                        (RowType) physicalDataType.getLogicalType(), typeInfo)
                                .setFieldDelimiter(delimiter)
                                .build());
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }
}
