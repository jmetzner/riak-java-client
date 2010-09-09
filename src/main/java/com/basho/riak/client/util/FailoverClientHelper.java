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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpHost;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.protocol.Protocol;

import com.basho.riak.client.RiakFailoverConfig;
import com.basho.riak.client.request.RequestMeta;
import com.basho.riak.client.response.DefaultHttpResponse;
import com.basho.riak.client.response.HttpResponse;
import com.basho.riak.client.response.RiakIORuntimeException;

/**
 * Failover client
 */
public class FailoverClientHelper extends ClientHelper {
    
    Queue<HostConfiguration> httpClientHostConfigs = new ConcurrentLinkedQueue<HostConfiguration>();
    final RiakFailoverConfig failoverConfig;

    public FailoverClientHelper(RiakFailoverConfig config, String clientId) {
        super(config, clientId);
        this.failoverConfig = config;
        addRiakUrls(failoverConfig.getUrlList());
    }
    
    public void addRiakUrls(String[] urlList) {
        for (String urlString:urlList) {
            try {
                URL url = new URL(urlString);
                addRiakHost(url.getHost(), url.getPort(), "https".equals(url.getProtocol()));
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException("malformed riak url: "+urlString, e);
            }
        }
    }
    
    public void addRiakHost(String host, int port, boolean usessl) {
        HostConfiguration config = new HostConfiguration();
        config.setHost(new HttpHost(host, port, usessl ? Protocol.getProtocol("https"):Protocol.getProtocol("http")));
        httpClientHostConfigs.add(config);
        // add to RiakFailoverConfig ?
    }
    
    public void removeRiakHost(String host, int port) {
        for (HostConfiguration config:httpClientHostConfigs) {
            if (host.equals(config.getHost()) && port == config.getPort()) {
                httpClientHostConfigs.remove(config);
                postProcessHostRemove(host, port);
            }
        }
        // remove from RiakFailoverConfig ?
    }
    
    /**
     * get the next host in the cluster.
     * @return the next host in the cluster.
     */
    HostConfiguration getNextHost() {
        if (this.httpClientHostConfigs.size() == 0) {
            return null;
        }
        return this.httpClientHostConfigs.peek();
    }
    
    /**
     * do some post processing when a host is removed.
     * @param host the host that was removed
     * @param port the corresponding port
     */
    void postProcessHostRemove(String host, int port) {
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
            
            int attempts = 0;
            int startSize = this.httpClientHostConfigs.size();
            HostConfiguration currentHostConfig = null;
            IOException ex = null;
            
            while (true) {
            
                try {
                    int size = this.httpClientHostConfigs.size();
                    if (size < 1) {
                        toss(new RiakIORuntimeException("No live riak servers available to handle this request"));
                    }
                    
                    currentHostConfig = getNextHost();
                    getHttpClient().executeMethod(currentHostConfig, httpMethod);
    
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
                    ex = e;
                    removeRiakHost(currentHostConfig.getHost(), currentHostConfig.getPort());
                }
                
                attempts++;
                if (attempts >= startSize) {
                    toss(new RiakIORuntimeException("No live riak servers available to handle this request", ex));
                }
            }
        } finally {
            if (!streamResponse) {
                httpMethod.releaseConnection();
            }
        }
    }
}
