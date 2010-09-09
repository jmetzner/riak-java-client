package com.basho.riak.client.util;

import java.io.IOException;

import org.apache.commons.httpclient.HttpClient;
import org.json.JSONObject;

import com.basho.riak.client.IRiakConfig;
import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.response.BucketResponse;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.RiakExceptionHandler;
import com.basho.riak.client.response.RiakIORuntimeException;
import com.basho.riak.client.response.RiakResponseRuntimeException;
import com.basho.riak.client.response.StreamHandler;

public interface IClientHelper {

    /**
     * See {@link RiakClient#getClientId()}
     */
    public abstract byte[] getClientId();

    public abstract void setClientId(String clientId);

    /**
     * See
     * {@link RiakClient#setBucketSchema(String, com.basho.riak.client.RiakBucketInfo, RequestMeta)}
     */
    public abstract HttpResponse setBucketSchema(String bucket, JSONObject schema, RequestMeta meta);

    /**
     * Same as {@link RiakClient#getBucketSchema(String, RequestMeta)}, except
     * only returning the HTTP response.
     */
    public abstract HttpResponse getBucketSchema(String bucket, RequestMeta meta);

    /**
     * Same as {@link RiakClient}, except only returning the HTTP response, and
     * if streamResponse==true, the response will be streamed back, so the user
     * is responsible for calling {@link BucketResponse#close()}
     */
    public abstract HttpResponse listBucket(String bucket, RequestMeta meta, boolean streamResponse);

    /**
     * Same as {@link RiakClient}, except only returning the HTTP response
     */
    public abstract HttpResponse store(RiakObject object, RequestMeta meta);

    /**
     * Same as {@link RiakClient}, except only returning the HTTP response
     */
    public abstract HttpResponse fetchMeta(String bucket, String key, RequestMeta meta);

    /**
     * Same as {@link RiakClient}, except only returning the HTTP response and
     * allows the response to be streamed.
     * 
     * @param bucket
     *            Same as {@link RiakClient}
     * @param key
     *            Same as {@link RiakClient}
     * @param meta
     *            Same as {@link RiakClient}
     * @param streamResponse
     *            If true, the connection will NOT be released. Use
     *            HttpResponse.getHttpMethod().getResponseBodyAsStream() to get
     *            the response stream; HttpResponse.getBody() will return null.
     * 
     * @return Same as {@link RiakClient}
     */
    public abstract HttpResponse fetch(String bucket, String key, RequestMeta meta, boolean streamResponse);

    public abstract HttpResponse fetch(String bucket, String key, RequestMeta meta);

    /**
     * Same as {@link RiakClient}, except only returning the HTTP response
     */
    public abstract boolean stream(String bucket, String key, StreamHandler handler, RequestMeta meta)
            throws IOException;

    /**
     * Same as {@link RiakClient}, except only returning the HTTP response
     */
    public abstract HttpResponse delete(String bucket, String key, RequestMeta meta);

    /**
     * Same as {@link RiakClient}, except only returning the HTTP response
     */
    public abstract HttpResponse walk(String bucket, String key, String walkSpec, RequestMeta meta);

    /**
     * Same as {@link RiakClient}, except only returning the HTTP response
     */
    public abstract HttpResponse mapReduce(String job, RequestMeta meta);

    /** @return the installed exception handler or null if not installed */
    public abstract RiakExceptionHandler getExceptionHandler();

    /**
     * Install an exception handler. If an exception handler is provided, then
     * the Riak client will hand exceptions to the handler rather than throwing
     * them.
     */
    public abstract void setExceptionHandler(RiakExceptionHandler exceptionHandler);

    /**
     * Hands exception <code>e</code> to installed exception handler if there is
     * one or throw it.
     * 
     * @return A 0-status {@link HttpResponse}.
     */
    public abstract HttpResponse toss(RiakIORuntimeException e);

    public abstract HttpResponse toss(RiakResponseRuntimeException e);

    /**
     * Return the {@link HttpClient} used to make requests, which can be
     * configured.
     */
    public abstract HttpClient getHttpClient();

    /**
     * @return The config used to construct the HttpClient connecting to Riak.
     */
    public abstract IRiakConfig getConfig();

}