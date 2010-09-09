package com.basho.riak.client;


/**
 * Configuration settings for connecting to a Riak cluster.
 */
public class RiakFailoverConfig extends RiakConfig {
    
    private String[] urlList;
    
    public RiakFailoverConfig(String[] urlList, String prefix) {
        this.urlList = urlList;
        setUrl(prefix == null ? DEFAULT_PREFIX : prefix);
    }
    
    public String[] getUrlList() {
        return this.urlList;
    }

}
