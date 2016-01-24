package com.springapp.mvc;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;

/**
 * Created by jenny on 16/1/18.q
 */
public class EsTest {
    private static Client client;
    public static void init(){
//        client = TransportClient.builder().build()
//                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300))
//                .addTransportAddress(new InetSocketTransportAddress("host2", 9300));
    }
}
