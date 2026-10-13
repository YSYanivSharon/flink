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

package org.apache.flink.connector.tcp.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;

/**
 * Each subtask connects to {@code hostname:port} and writes one {@code <RowKind>,<csv>} line per
 * row.
 */
public final class TcpSink implements Sink<RowData> {

    private final String hostname;
    private final int port;
    private final long flushIntervalMs;
    private final RowType rowType;

    public TcpSink(String hostname, int port, long flushIntervalMs, RowType rowType) {
        this.hostname = hostname;
        this.port = port;
        this.flushIntervalMs = flushIntervalMs;
        this.rowType = rowType;
    }

    @Override
    public SinkWriter<RowData> createWriter(WriterInitContext context) throws IOException {
        return new TcpSinkWriter(
                hostname, port, flushIntervalMs, rowType, context.getProcessingTimeService());
    }
}
