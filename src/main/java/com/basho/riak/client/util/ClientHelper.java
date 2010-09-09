/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.json.JSONObject;

import com.basho.riak.client.IRiakConfig;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.response.DefaultHttpResponse;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.RiakExceptionHandler;
import com.basho.riak.client.response.RiakIORuntimeException;
import com.basho.riak.client.response.RiakResponseRuntimeException;
import com.basho.riak.client.response.StreamHandler;

/**
 * This class performs the actual HTTP requests underlying the operations in
 * RiakClient and returns the resulting HTTP responses. It is up to RiakClient
 * to interpret the responses and translate them into the appropriate format.
 */
public class ClientHelper implements IClientHelper {

    private IRiakConfig config;
    private HttpClient httpClient;
    private String clientId = null;
    private RiakExceptionHandler exceptionHandler = null;

    public ClientHelper(IRiakConfig config, String clientId) {
        this.config = config;
        httpClient = ClientUtils.newHttpClient(config);
        setClientId(clientId);
    }

    /** Used for testing -- inject an HttpClient */
    void setHttpClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.util.IClientHelper#getClientId()
     */
    public byte[] getClientId() {
        try {
            return Base64.decodeBase64(clientId.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 support required in JVM");
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.util.IClientHelper#setClientId(java.lang.String)
     */
    public void setClientId(String clientId) {
        if (clientId != null) {
            this.clientId = ClientUtils.encodeClientId(clientId);
        } else {
            this.clientId = ClientUtils.randomClientId();
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.util.IClientHelper#setBucketSchema(java.lang.String, org.json.JSONObject, com.basho.riak.client.request.RequestMeta)
     */
    public HttpResponse setBucketSchema(String bucket, JSONObject schema, RequestMeta meta) {
        if (schema == null) {
            schema = new JSONObject();
        }
        if (meta == null) {
            meta = new RequestMeta();
        }

        meta.setHeader(Constants.HDR_ACCEPT, Constants.CTYPE_JSON);

        PutMethod put = new PutMethod(ClientUtils.makeURI(config, bucket));
        put.setRequestEntity(new ByteArrayRequestEntity(schema.toString().getBytes(), Constants.CTYPE_JSON));

        return executeMethod(bucket, null, put, meta);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.util.IClientHelper#getBucketSchema(java.lang.String, com.basho.riak.client.request.RequestMeta)
     */
    public HttpResponse getBucketSchema(String bucket, RequestMeta meta) {
        if (meta == null) {
            meta = new RequestMeta();
        }
        if (meta.getQueryParam(Constants.QP_KEYS) == null) {
            meta.setQueryParam(Constants.QP_KEYS, Constants.NO_KEYS);
        }
        return listBucket(bucket, meta, false);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.util.IClientHelper#listBucket(java.lang.String, com.basho.riak.client.request.RequestMeta, boolean)
     */
    public HttpResponse listBucket(String bucket, RequestMeta meta, boolean streamResponse) {
        if (meta == null) {
            meta = new RequestMeta();
        }
        if (meta.getQueryParam(Constants.QP_KEYS) == null) {
            if (streamResponse) {
                meta.setQueryParam(Constants.QP_KEYS, Constants.STREAM_KEYS);
            } else {
                meta.setQueryParam(Constants.QP_KEYS, Constants.INCLUDE_KEYS);
            }
        }
        if (meta.getHeader(Constants.HDR_CONTENT_TYPE) == null) {
            meta.setHeader(Constants.HDR_CONTENT_TYPE, Constants.CTYPE_JSON);
        }
        if (meta.getHeader(Constants.HDR_ACCEPT) == null) {
            meta.setHeader(Constants.HDR_ACCEPT, Constants.CTYPE_JSON);
        }

        GetMethod get = new GetMethod(ClientUtils.makeURI(config, bucket));
        return executeMethod(bucket, null, get, meta, streamResponse);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.util.IClientHelper#store(com.basho.riak.client.RiakObject, com.basho.riak.client.request.RequestMeta)
     */
    public HttpResponse store(RiakObject object, RequestMeta meta) {
        if (meta == null) {
            meta = new RequestMeta();
        }
        if (meta.getClientId() == null) {
            meta.setClientId(clientId);
        }
        if (meta.getHeader(Constants.HDR_CONNECTION) == null) {
            meta.setHeader(Constants.HDR_CONNECTION, "keep-alive");
        }

        String bucket = object.getBucket();
        String key = object.getKey();
        String url = ClientUtils.makeURI(config, bucket, key);
        PutMethod put = new PutMethod(url);

        object.writeToHttpMethod(put);
        return executeMethod(bucket, key, put, meta);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.util.IClientHelper#fetchMeta(java.lang.String, java.lang.String, com.basho.riak.client.request.RequestMeta)
     */
    public HttpResponse fetchMeta(String bucket, String key, RequestMeta meta) {
        if (meta == null) {
            meta = new RequestMeta();
        }
        if (meta.getQueryParam(Constants.QP_R) == null) {
            meta.setQueryParam(Constants.QP_R, Constants.DEFAULT_R.toString());
        }
        HeadMethod head = new HeadMethod(ClientUtils.makeURI(config, bucket, key));
        return executeMethod(bucket, key, head, meta);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.util.IClientHelper#fetch(java.lang.String, java.lang.String, com.basho.riak.client.request.RequestMeta, boolean)
     */
    public HttpResponse fetch(String bucket, String key, RequestMeta meta, boolean streamResponse) {
        if (meta == null) {
            meta = new RequestMeta();
        }
        if (meta.getQueryParam(Constants.QP_R) == null) {
            meta.setQueryParam(Constants.QP_R, Constants.DEFAULT_R.toString());
        }
        GetMethod get = new GetMethod(ClientUtils.makeURI(config, bucket, key));
        return executeMethod(bucket, key, get, meta, streamResponse);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.util.IClientHelper#fetch(java.lang.String, java.lang.String, com.basho.riak.client.request.RequestMeta)
     */
    public HttpResponse fetch(String bucket, String key, RequestMeta meta) {
        return fetch(bucket, key, meta, false);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.util.IClientHelper#stream(java.lang.String, java.lang.String, com.basho.riak.client.response.StreamHandler, com.basho.riak.client.request.RequestMeta)
     */
    public boolean stream(String bucket, String key, StreamHandler handler, RequestMeta meta) throws IOException {
        if (meta == null) {
            meta = new RequestMeta();
        }
        if (meta.getQueryParam(Constants.QP_R) == null) {
            meta.setQueryParam(Constants.QP_R, Constants.DEFAULT_R.toString());
        }
        GetMethod get = new GetMethod(ClientUtils.makeURI(config, bucket, key));
        try {
            int status = httpClient.executeMethod(get);
            if (handler == null)
                return true;

            return handler.process(bucket, key, status, ClientUtils.asHeaderMap(get.getResponseHeaders()),
                                   get.getResponseBodyAsStream(), get);
        } finally {
            get.releaseConnection();
        }
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.util.IClientHelper#delete(java.lang.String, java.lang.String, com.basho.riak.client.request.RequestMeta)
     */
    public HttpResponse delete(String bucket, String key, RequestMeta meta) {
        if (meta == null) {
            meta = new RequestMeta();
        }
        String url = ClientUtils.makeURI(config, bucket, key);
        DeleteMethod delete = new DeleteMethod(url);
        return executeMethod(bucket, key, delete, meta);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.util.IClientHelper#walk(java.lang.String, java.lang.String, java.lang.String, com.basho.riak.client.request.RequestMeta)
     */
    public HttpResponse walk(String bucket, String key, String walkSpec, RequestMeta meta) {
        GetMethod get = new GetMethod(ClientUtils.makeURI(config, bucket, key, walkSpec));
        return executeMethod(bucket, key, get, meta);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.util.IClientHelper#mapReduce(java.lang.String, com.basho.riak.client.request.RequestMeta)
     */
    public HttpResponse mapReduce(String job, RequestMeta meta) {
        PostMethod post = new PostMethod(config.getMapReduceUrl());
        try {
            post.setRequestEntity(new StringRequestEntity(job, Constants.CTYPE_JSON, null));
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("StringRequestEntity should always support no charset", e);
        }
        return executeMethod(null, null, post, meta);
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.util.IClientHelper#getExceptionHandler()
     */
    public RiakExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.util.IClientHelper#setExceptionHandler(com.basho.riak.client.response.RiakExceptionHandler)
     */
    public void setExceptionHandler(RiakExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.util.IClientHelper#toss(com.basho.riak.client.response.RiakIORuntimeException)
     */
    public HttpResponse toss(RiakIORuntimeException e) {
        if (exceptionHandler != null) {
            exceptionHandler.handle(e);
            return new DefaultHttpResponse(null, null, 0, null, null, null, null);
        } else
            throw e;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.util.IClientHelper#toss(com.basho.riak.client.response.RiakResponseRuntimeException)
     */
    public HttpResponse toss(RiakResponseRuntimeException e) {
        if (exceptionHandler != null) {
            exceptionHandler.handle(e);
            return new DefaultHttpResponse(null, null, 0, null, null, null, null);
        } else
            throw e;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.util.IClientHelper#getHttpClient()
     */
    public HttpClient getHttpClient() {
        return httpClient;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.util.IClientHelper#getConfig()
     */
    public IRiakConfig getConfig() {
        return config;
    }

    /**
     * Perform and HTTP request and return the resulting response using the
     * internal HttpClient.
     * 
     * @param bucket
     *            Bucket of the object receiving the request.
     * @param key
     *            Key of the object receiving the request or null if the request
     *            is for a bucket.
     * @param httpMethod
     *            The HTTP request to perform; must not be null.
     * @param meta
     *            Extra HTTP headers to attach to the request. Query parameters
     *            are ignored; they should have already been used to construct
     *            <code>httpMethod</code> and query parameters.
     * @param streamResponse
     *            If true, the connection will NOT be released. Use
     *            HttpResponse.getHttpMethod().getResponseBodyAsStream() to get
     *            the response stream; HttpResponse.getBody() will return null.
     * 
     * @return The HTTP response returned by Riak from executing
     *         <code>httpMethod</code>.
     * 
     * @throws RiakIORuntimeException
     *             If an error occurs during communication with the Riak server
     *             (i.e. HttpClient threw an IOException)
     */
    HttpResponse executeMethod(String bucket, String key, HttpMethod httpMethod, RequestMeta meta,
                               boolean streamResponse) {

        if (meta != null) {
            Map<String, String> headers = meta.getHeaders();
            for (String header : headers.keySet()) {
                httpMethod.setRequestHeader(header, headers.get(header));
            }

            String queryParams = meta.getQueryParams();
            if (queryParams != null && (queryParams.length() != 0)) {
                String currentQuery = httpMethod.getQueryString();
                if (currentQuery != null && (currentQuery.length() != 0)) {
                    httpMethod.setQueryString(currentQuery + "&" + queryParams);
                } else {
                    httpMethod.setQueryString(queryParams);
                }
            }
        }

        try {
            httpClient.executeMethod(httpMethod);

            int status = 0;
            if (httpMethod.getStatusLine() != null) {
                status = httpMethod.getStatusCode();
            }

            Map<String, String> headers = ClientUtils.asHeaderMap(httpMethod.getResponseHeaders());
            byte[] body = null;
            InputStream stream = null;
            if (streamResponse) {
                stream = httpMethod.getResponseBodyAsStream();
            } else {
                body = httpMethod.getResponseBody();
            }

            return new DefaultHttpResponse(bucket, key, status, headers, body, stream, httpMethod);
        } catch (IOException e) {
            return toss(new RiakIORuntimeException(e));
        } finally {
            if (!streamResponse) {
                httpMethod.releaseConnection();
            }
        }
    }

    HttpResponse executeMethod(String bucket, String key, HttpMethod httpMethod, RequestMeta meta) {
        return executeMethod(bucket, key, httpMethod, meta, false);
    }
}
