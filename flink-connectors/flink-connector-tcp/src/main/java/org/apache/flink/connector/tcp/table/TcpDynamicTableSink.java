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

import org.apache.flink.connector.tcp.sink.TcpSink;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.types.logical.RowType;

/** Writes every changelog row to a TCP server as {@code <RowKind>,<csv>}. */
public final class TcpDynamicTableSink implements DynamicTableSink {

    private final String hostname;
    private final int port;
    private final long flushIntervalMs;
    private final RowType rowType;

    public TcpDynamicTableSink(String hostname, int port, long flushIntervalMs, RowType rowType) {
        this.hostname = hostname;
        this.port = port;
        this.flushIntervalMs = flushIntervalMs;
        this.rowType = rowType;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.all();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkV2Provider.of(new TcpSink(hostname, port, flushIntervalMs, rowType));
    }

    @Override
    public DynamicTableSink copy() {
        return new TcpDynamicTableSink(hostname, port, flushIntervalMs, rowType);
    }

    @Override
    public String asSummaryString() {
        return "TCP Table Sink";
    }
}
