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

public class Base24SourceConfigurationConstants {

    /**
     * Hostname to bind to.
     */
    public static final String CONFIG_HOSTNAME = "bind";

    /**
     * Port to bind to.
     */
    public static final String CONFIG_PORT = "port";


    /**
     * Attempt to answer the received message via base24 response handler logic
     */
    public static final String BASE24_INTERACTIVE = "b24-interactive";

    /**
     * Maximum line length per event.
     */
    public static final String BUFFER_SIZE = "bufferSize";
    public static final int DEFAULT_BUFFER_SIZE = 8192;

    public static final String OUTPUT_FORMAT = "outputFormat";
    public static final String DEFAULT_OUTPUT_FORMAT = "base24";

}

