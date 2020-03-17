package com.woombatcg.hadoop.flume.source;

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
     * Buffer
     */
    public static final String BUFFER_SIZE = "bufferSize";
    public static final int DEFAULT_BUFFER_SIZE = 8192;

    public static final String OUTPUT_FORMAT = "outputFormat";
    public static final String DEFAULT_OUTPUT_FORMAT = "base24";

}

