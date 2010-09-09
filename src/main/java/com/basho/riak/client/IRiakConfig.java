package com.basho.riak.client;

import java.util.regex.Pattern;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethodRetryHandler;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;

public interface IRiakConfig {

    public static final Pattern BASE_URL_PATTERN = Pattern.compile("^((?:[^:]*://)?[^/]*)");

    /**
     * The base URL used by a client to construct object URLs
     */
    public String getUrl();

    /**
     * Set the base URL that clients should use to construct object URLs (e.g.
     * http://localhost:8098/riak).
     */
    public void setUrl(String url);

    /**
     * The full URL of Riak map reduce resource, which is calculated by
     * combining the host and port from the Riak URL and the map reduce path.
     */
    public String getMapReduceUrl();

    /**
     * The host and port of the Riak server, which is extracted from the
     * specified Riak URL.
     */
    public String getBaseUrl();

    /**
     * The path to the Riak map reduce resource, which defaults to /mapred
     */
    public String getMapReducePath();

    public void setMapReducePath(String path);

    /**
     * The pre-constructed HttpClient for a client to use if one was provided
     */
    public HttpClient getHttpClient();

    /**
     * Provide a pre-constructed HttpClient for clients to use to connect to
     * Riak
     */
    public void setHttpClient(HttpClient httpClient);

    /**
     * Value to set for the properties:
     * {@link HttpClientParams#CONNECTION_MANAGER_TIMEOUT},
     * {@link HttpClientParams#SO_TIMEOUT},
     * {@link HttpConnectionManagerParams#CONNECTION_TIMEOUT} which sets the
     * timeout milliseconds for retrieving an HTTP connection and data over the
     * connection. Null for default.
     */
    public void setTimeout(final Integer timeout);

    public Integer getTimeout();

    /**
     * Value to set for the HttpConnectionManagerParams.MAX_TOTAL_CONNECTIONS
     * property: overall maximum number of connections used by the HttpClient.
     */
    public void setMaxConnections(Integer maxConnections);

    public Integer getMaxConnections();

    /**
     * Value to set for the HttpClientParams.RETRY_HANDLER property: the default
     * retry handler for requests.
     * 
     * @see org.apache.commons.httpclient.DefaultHttpMethodRetryHandler
     */
    public HttpMethodRetryHandler getRetryHandler();

    public void setRetryHandler(HttpMethodRetryHandler retryHandler);

}