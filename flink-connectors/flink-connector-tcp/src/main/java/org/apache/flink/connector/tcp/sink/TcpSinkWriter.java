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

import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Timestamps are written as epoch milliseconds so the reader can compute latencies. */
final class TcpSinkWriter implements SinkWriter<RowData> {

    private static final int CONNECT_ATTEMPTS = 60;

    private final Socket socket;
    private final OutputStream out;
    private final long flushIntervalMs;
    private final ProcessingTimeService timers;
    private final LogicalType[] types;
    private final RowData.FieldGetter[] getters;
    private final StringBuilder line = new StringBuilder();
    private volatile boolean closed;

    TcpSinkWriter(
            String hostname,
            int port,
            long flushIntervalMs,
            RowType rowType,
            ProcessingTimeService timers)
            throws IOException {
        this.socket = connect(hostname, port);
        this.out = new BufferedOutputStream(socket.getOutputStream(), 1 << 16);
        this.flushIntervalMs = flushIntervalMs;
        this.timers = timers;
        List<LogicalType> fields = rowType.getChildren();
        this.types = fields.toArray(new LogicalType[0]);
        this.getters = new RowData.FieldGetter[types.length];
        for (int i = 0; i < types.length; i++) {
            getters[i] = RowData.createFieldGetter(types[i], i);
        }
        if (flushIntervalMs > 0) {
            scheduleFlush();
        }
    }

    @Override
    public void write(RowData row, Context context) throws IOException {
        line.setLength(0);
        line.append(row.getRowKind().shortString());
        for (int i = 0; i < getters.length; i++) {
            line.append(',');
            append(getters[i].getFieldOrNull(row), types[i]);
        }
        line.append('\n');
        out.write(line.toString().getBytes(StandardCharsets.UTF_8));
        if (flushIntervalMs == 0) {
            out.flush();
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        out.flush();
    }

    @Override
    public void close() throws Exception {
        closed = true;
        try {
            out.flush();
        } finally {
            socket.close();
        }
    }

    private void append(Object value, LogicalType type) {
        if (value == null) {
            return;
        }
        switch (type.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                line.append(((TimestampData) value).getMillisecond());
                break;
            default:
                line.append(value);
        }
    }

    private void scheduleFlush() {
        timers.registerTimer(
                timers.getCurrentProcessingTime() + flushIntervalMs,
                time -> {
                    if (!closed) {
                        out.flush();
                        scheduleFlush();
                    }
                });
    }

    private static Socket connect(String hostname, int port) throws IOException {
        for (int attempt = 1; ; attempt++) {
            Socket socket = new Socket();
            try {
                socket.setTcpNoDelay(true);
                socket.connect(new InetSocketAddress(hostname, port));
                return socket;
            } catch (IOException e) {
                socket.close();
                if (attempt == CONNECT_ATTEMPTS) {
                    throw new IOException("Could not connect to " + hostname + ":" + port, e);
                }
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("Interrupted while connecting");
            }
        }
    }
}
