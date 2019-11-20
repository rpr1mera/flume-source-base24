package com.woombatcg.hadoop.flume.source;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.*;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

//import com.woombatcg.hadoop.util.base24.Functions;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.Source;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * <p>
 * A Base24-like source that listens on a given port and turns each line of text
 * into an event.
 * </p>
 * <p>
 * This source, primarily built for testing and exceedingly simple systems, acts
 * like <tt>nc -k -l [host] [port]</tt>. In other words, it opens a specified
 * port and listens for data. The expectation is that the supplied data is
 * newline separated text. Each line of text is turned into a Flume event and
 * sent via the connected channel.
 * </p>
 * <p>
 * Most testing has been done by using the <tt>nc</tt> client but other,
 * similarly implemented, clients should work just fine.
 * </p>
 * <p>
 * <b>Configuration options</b>
 * </p>
 * <table>
 * <tr>
 * <th>Parameter</th>
 * <th>Description</th>
 * <th>Unit / Type</th>
 * <th>Default</th>
 * </tr>
 * <tr>
 * <td><tt>bind</tt></td>
 * <td>The hostname or IP to which the source will bind.</td>
 * <td>Hostname or IP / String</td>
 * <td>none (required)</td>
 * </tr>
 * <tr>
 * <td><tt>port</tt></td>
 * <td>The port to which the source will bind and listen for events.</td>
 * <td>TCP port / int</td>
 * <td>none (required)</td>
 * </tr>
 * <tr>
 * <td><tt>max-line-length</tt></td>
 * <td>The maximum # of chars a line can be per event (including newline).</td>
 * <td>Number of ascii characters / int</td>
 * <td>512</td>
 * </tr>
 * </table>
 * <p>
 * <b>Metrics</b>
 * </p>
 * <p>
 * TODO
 * </p>
 */
public class Base24Source extends AbstractSource implements Configurable,
        EventDrivenSource {

    private static final Logger logger = LoggerFactory
            .getLogger(Base24Source.class);

    private String hostName;
    private int port;
    private int maxLineLength;
    private boolean ackEveryEvent;
    private String hexStartOfMessage;

    private CounterGroup counterGroup;
    private ServerSocketChannel serverSocketChannel;
    private SocketChannel socketChannel;
    private AtomicBoolean acceptThreadShouldStop;
    private Thread acceptThread;
    private ExecutorService handlerService;
//    private Functions utilities;

    public Base24Source() {
        super();

        port = 0;
        counterGroup = new CounterGroup();
        acceptThreadShouldStop = new AtomicBoolean(false);
    }

    @Override
    public void configure(Context context) {
        String hostKey = Base24SourceConfigurationConstants.CONFIG_HOSTNAME;
        String portKey = Base24SourceConfigurationConstants.CONFIG_PORT;
        String ackEventKey = Base24SourceConfigurationConstants.CONFIG_ACKEVENT;
        String hexStartOfMessageKey = Base24SourceConfigurationConstants.HEX_START_OF_MESSAGE;

        Configurables.ensureRequiredNonNull(context, hostKey, portKey);

        hostName = context.getString(hostKey);
        port = context.getInteger(portKey);
        ackEveryEvent = context.getBoolean(ackEventKey, true);
        maxLineLength = context.getInteger(
                Base24SourceConfigurationConstants.CONFIG_MAX_LINE_LENGTH,
                Base24SourceConfigurationConstants.DEFAULT_MAX_LINE_LENGTH
        );

//        utilities = new Functions();
        hexStartOfMessage = context.getString(
                Base24SourceConfigurationConstants.HEX_START_OF_MESSAGE,
                Base24SourceConfigurationConstants.DEFAULT_HEX_START_OF_MESSAGE
        );
    }

    @Override
    public void start() {

        logger.info("Source starting");

        counterGroup.incrementAndGet("open.attempts");

        handlerService = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                .setNameFormat("base24-handler-%d").build());

        try {
            SocketAddress bindPoint = new InetSocketAddress(hostName, port);

            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.socket().setReuseAddress(true);
            serverSocketChannel.socket().bind(bindPoint);

            logger.info("Listening on: {}", serverSocketChannel);
        } catch (IOException e) {
            counterGroup.incrementAndGet("open.errors");
            logger.error("Unable to bind to socket. Exception follows.", e);
            throw new FlumeException(e);
        }

        AcceptHandler acceptRunnable = new AcceptHandler(maxLineLength);
        acceptThreadShouldStop.set(false);
        acceptRunnable.counterGroup = counterGroup;
        acceptRunnable.handlerService = handlerService;
        acceptRunnable.shouldStop = acceptThreadShouldStop;
        acceptRunnable.ackEveryEvent = ackEveryEvent;
        acceptRunnable.source = this;
        acceptRunnable.serverSocketChannel = serverSocketChannel;

        acceptThread = new Thread(acceptRunnable);

        acceptThread.start();

        logger.debug("Source started");
        super.start();
    }

    @Override
    public void stop() {
        logger.info("Source stopping");

        acceptThreadShouldStop.set(true);

        if (acceptThread != null) {
            logger.debug("Stopping accept handler thread");

            while (acceptThread.isAlive()) {
                try {
                    logger.debug("Waiting for accept handler to finish");
                    acceptThread.interrupt();
                    acceptThread.join(500);
                } catch (InterruptedException e) {
                    logger
                            .debug("Interrupted while waiting for accept handler to finish");
                    Thread.currentThread().interrupt();
                }
            }

            logger.debug("Stopped accept handler thread");
        }

        if (serverSocketChannel != null) {
            try {
                serverSocketChannel.close();
            } catch (IOException e) {
                logger.error("Unable to close socket. Exception follows.", e);
                return;
            }
        }

        if (handlerService != null) {
            handlerService.shutdown();

            logger.debug("Waiting for handler service to stop");

            // wait 500ms for threads to stop
            try {
                handlerService.awaitTermination(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger
                        .debug("Interrupted while waiting for Base24 handler service to stop");
                Thread.currentThread().interrupt();
            }

            if (!handlerService.isShutdown()) {
                handlerService.shutdownNow();
            }

            logger.debug("Handler service stopped");
        }

        logger.debug("Source stopped. Event metrics:{}", counterGroup);
        super.stop();
    }

    private static class AcceptHandler implements Runnable {

        private ServerSocketChannel serverSocketChannel;
        private CounterGroup counterGroup;
        private ExecutorService handlerService;
        private EventDrivenSource source;
        private AtomicBoolean shouldStop;
        private boolean ackEveryEvent;

        private final int maxLineLength;

        public AcceptHandler(int maxLineLength) {
            this.maxLineLength = maxLineLength;
        }

        @Override
        public void run() {
            logger.debug("Starting accept handler");

            while (!shouldStop.get()) {
                try {
                    SocketChannel socketChannel = serverSocketChannel.accept();

                    Base24SocketHandler request = new Base24SocketHandler(maxLineLength);

                    request.socketChannel = socketChannel;
                    request.counterGroup = counterGroup;
                    request.source = source;
                    request.ackEveryEvent = ackEveryEvent;

                    handlerService.submit(request);

                    counterGroup.incrementAndGet("accept.succeeded");
                } catch (ClosedByInterruptException e) {
                    // Parent is canceling us.
                } catch (IOException e) {
                    logger.error("Unable to accept connection. Exception follows.", e);
                    counterGroup.incrementAndGet("accept.failed");
                }
            }

            logger.debug("Accept handler exiting");
        }
    }

    private static class Base24SocketHandler implements Runnable {

        private Source source;
        private CounterGroup counterGroup;
        private SocketChannel socketChannel;
        private boolean ackEveryEvent;

        private final int maxLineLength;

        public Base24SocketHandler(int maxLineLength) {
            this.maxLineLength = maxLineLength;
        }

        /**
         * <p>Refill the buffer read from the socket.</p>
         * <p>
         * Preconditions: <br/>
         * buffer should have position @ beginning of unprocessed data. <br/>
         * buffer should have limit @ end of unprocessed data. <br/>
         * <p>
         * Postconditions: <br/>
         * buffer should have position @ beginning of buffer (pos=0). <br/>
         * buffer should have limit @ end of unprocessed data. <br/>
         * <p>
         * Note: this method blocks on new data arriving.
         *
         * @param buffer The buffer to fill
         * @param socketChannel The SocketChannel to read the data from
         * @return number of characters read
         * @throws IOException
         */
        private int fill(ByteBuffer buffer, SocketChannel socketChannel)
                throws IOException {

            // move existing data to the front of the buffer
            buffer.compact();

            // pull in as much data as we can from the socket
//            int charsRead = reader.read(buffer);
            int bytesRead = socketChannel.read(buffer);

            counterGroup.addAndGet("characters.received", Long.valueOf(bytesRead));

            // flip so the data can be consumed
            buffer.flip();

            return bytesRead;
        }

        @Override
        public void run() {
            logger.debug("Starting connection handler");
            Event event = null;

            try {
                ByteBuffer buffer = ByteBuffer.allocate(maxLineLength);
                buffer.flip(); // flip() so fill() sees buffer as initially empty

                while (true) {
                    // this method blocks until new data is available in the socket
                    int bytesRead = fill(buffer, socketChannel);
                    logger.debug("Chars read = {}", bytesRead);

                    if (bytesRead == -1) {
                        // if we received EOF before last event processing attempt, then we
                        // have done everything we can
                        break;
                    }

                    // attempt to process all the events in the buffer
                    int eventsProcessed = processEvents(buffer, socketChannel);
                    logger.debug("Events processed = {}", eventsProcessed);
                }

                socketChannel.close();

                counterGroup.incrementAndGet("sessions.completed");
            } catch (IOException e) {
                logger.debug("Caught IOException while trying to prepare the buffers:");
                e.printStackTrace();
                counterGroup.incrementAndGet("sessions.broken");
            }

            logger.debug("Connection handler exiting");
        }

        /**
         * <p>Consume some number of events from the buffer into the system.</p>
         * <p>
         * Invariants (pre- and post-conditions): <br/>
         * buffer should have position @ beginning of unprocessed data. <br/>
         * buffer should have limit @ end of unprocessed data. <br/>
         *
         * @param buffer The buffer containing data to process
         * @param socketChannel The channel back to the client
         * @return number of events successfully processed
         * @throws IOException
         */
        private int processEvents(ByteBuffer buffer, SocketChannel socketChannel)
                throws IOException {

            int numProcessed = 0;
            int limit = buffer.limit();

            byte[] body = new byte[buffer.remaining()];
            buffer.get(body);

            String s = new String(body, StandardCharsets.US_ASCII);
            logger.debug("ASCII Encoding" + s);
//            System.out.print(new String(body, StandardCharsets.US_ASCII));

            Event event = EventBuilder.withBody(body);

            // process event
            ChannelException ex = null;
            try {
                source.getChannelProcessor().processEvent(event);
            } catch (ChannelException chEx) {
                ex = chEx;
            }

            if (ex == null) {
                counterGroup.incrementAndGet("events.processed");
                logger.warn(counterGroup.toString());
                numProcessed++;
                if (ackEveryEvent) {
                    // The base24 answer message should be written to the socket here.
//                    writer.write("OK\n");
                    socketChannel.write(ByteBuffer.wrap(body.clone()));
                }
            } else {
                counterGroup.incrementAndGet("events.failed");
                logger.warn("Error processing event. Exception follows.", ex);
//                writer.write("FAILED: " + ex.getMessage() + "\n");
            }
//            writer.flush();


        return numProcessed;
    }



}
}