package com.springapp.mvc;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by jenny on 16/1/22.
 */
public class EsUtil {
    private static TransportClient client;

    public static TransportClient getClient() {
        try {
            client = TransportClient.builder().settings(settings()).build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return client;
    }

    private static Settings settings() {
        return Settings.settingsBuilder().put("cluster.name", "es-cluster").put("client.transport.sniff", Boolean.TRUE).build();
    }
}
