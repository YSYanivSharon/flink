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

package org.apache.flink.connector.tcp.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;

import java.util.Set;

/** Factory for {@code 'connector' = 'tcp'}; both sides connect to a server as clients. */
public final class TcpDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname").stringType().noDefaultValue();

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port").intType().noDefaultValue();

    public static final ConfigOption<Integer> BYTE_DELIMITER =
            ConfigOptions.key("byte-delimiter")
                    .intType()
                    .defaultValue(10)
                    .withDescription("Source record delimiter; the default is '\\n'.");

    public static final ConfigOption<String> SOURCE_NAME =
            ConfigOptions.key("source-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "If set, the source writes this name and a newline after connecting.");

    public static final ConfigOption<Long> FLUSH_INTERVAL_MS =
            ConfigOptions.key("flush-interval-ms")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("Sink flush period; 0 flushes after every row.");

    @Override
    public String factoryIdentifier() {
        return "tcp";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of(HOSTNAME, PORT);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of(FactoryUtil.FORMAT, BYTE_DELIMITER, SOURCE_NAME, FLUSH_INTERVAL_MS);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(
                        DeserializationFormatFactory.class, FactoryUtil.FORMAT);
        helper.validate();
        final ReadableConfig options = helper.getOptions();
        return new TcpDynamicTableSource(
                options.get(HOSTNAME),
                options.get(PORT),
                (byte) (int) options.get(BYTE_DELIMITER),
                options.get(SOURCE_NAME),
                decodingFormat,
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType());
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        final ReadableConfig options = helper.getOptions();
        return new TcpDynamicTableSink(
                options.get(HOSTNAME),
                options.get(PORT),
                options.get(FLUSH_INTERVAL_MS),
                (RowType)
                        context.getCatalogTable()
                                .getResolvedSchema()
                                .toPhysicalRowDataType()
                                .getLogicalType());
    }
}
