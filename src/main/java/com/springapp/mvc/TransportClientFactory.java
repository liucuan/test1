package com.springapp.mvc;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.net.InetAddress;

/**
 * Created by jenny on 16/1/19.
 */
public class TransportClientFactory implements FactoryBean<TransportClient>, InitializingBean, DisposableBean {
    private static final Logger logger = LoggerFactory.getLogger(TransportClientFactory.class);
    private String clusterNodes = "127.0.0.1:9300";
    private String clusterName = "elasticsearch";
    private Boolean clientTransportSniff = Boolean.TRUE;
    private Boolean clientIgnoreClusterName = Boolean.FALSE;
    private String clientPingTimeout;
    private String clientNodesSamplerInterval;
    private TransportClient client;
    static final String COLON = ":";
    static final String COMMA = ",";

    public TransportClientFactory() {
        this.clientIgnoreClusterName = Boolean.FALSE;
        this.clientPingTimeout = "5s";
        this.clientNodesSamplerInterval = "5s";
    }

    public void destroy() throws Exception {
        try {
            logger.info("Closing elasticSearch  client");
            if(this.client != null) {
                this.client.close();
            }
        } catch (Exception var2) {
            logger.error("Error closing ElasticSearch client: ", var2);
        }

    }

    public TransportClient getObject() throws Exception {
        return this.client;
    }

    public Class<TransportClient> getObjectType() {
        return TransportClient.class;
    }

    public boolean isSingleton() {
        return false;
    }

    public void afterPropertiesSet() throws Exception {
        this.buildClient();
    }

    protected void buildClient() throws Exception {
        this.client = TransportClient.builder().settings(settings()).build();
        Assert.hasText(this.clusterNodes, "[Assertion failed] clusterNodes settings missing.");
        String[] var1 = StringUtils.split(this.clusterNodes, ",");
        int var2 = var1.length;

        for(int var3 = 0; var3 < var2; ++var3) {
            String clusterNode = var1[var3];
            String hostName = StringUtils.substringBefore(clusterNode, ":");
            String port = StringUtils.substringAfter(clusterNode, ":");
            Assert.hasText(hostName, "[Assertion failed] missing host name in \'clusterNodes\'");
            Assert.hasText(port, "[Assertion failed] missing port in \'clusterNodes\'");
            logger.info("adding transport node : " + clusterNode);
            this.client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName), Integer.valueOf(port).intValue()));
        }

        this.client.connectedNodes();
    }

    private Settings settings() {
        return Settings.settingsBuilder().put("cluster.name", this.clusterName).put("client.transport.sniff", this.clientTransportSniff.booleanValue()).put("client.transport.ignore_cluster_name", this.clientIgnoreClusterName.booleanValue()).put("client.transport.ping_timeout", this.clientPingTimeout).put("client.transport.nodes_sampler_interval", this.clientNodesSamplerInterval).build();
    }

    public void setClusterNodes(String clusterNodes) {
        this.clusterNodes = clusterNodes;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setClientTransportSniff(Boolean clientTransportSniff) {
        this.clientTransportSniff = clientTransportSniff;
    }

    public String getClientNodesSamplerInterval() {
        return this.clientNodesSamplerInterval;
    }

    public void setClientNodesSamplerInterval(String clientNodesSamplerInterval) {
        this.clientNodesSamplerInterval = clientNodesSamplerInterval;
    }

    public String getClientPingTimeout() {
        return this.clientPingTimeout;
    }

    public void setClientPingTimeout(String clientPingTimeout) {
        this.clientPingTimeout = clientPingTimeout;
    }

    public Boolean getClientIgnoreClusterName() {
        return this.clientIgnoreClusterName;
    }

    public void setClientIgnoreClusterName(Boolean clientIgnoreClusterName) {
        this.clientIgnoreClusterName = clientIgnoreClusterName;
    }

}
