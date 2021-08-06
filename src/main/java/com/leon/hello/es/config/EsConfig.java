package com.leon.hello.es.config;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;
@Slf4j
@Configuration
public class EsConfig {
    /*
    @Bean
    public TransportClient transportClient() throws UnknownHostException {
        InetSocketTransportAddress node = new InetSocketTransportAddress(
                InetAddress.getByName("localhost"),
                9300); // TCP 端口 9300，不是 HTTP 端口 9200

        Settings settings = Settings.builder()
                .put("cluster.name", "leon-es")
                .build();
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(node); // 可以添加多个节点
        return client;
    }
    */


    @Bean
    public TransportClient transportClient() throws UnknownHostException {
        InetSocketTransportAddress node = new InetSocketTransportAddress(
                InetAddress.getByName("92.168.11.197"),
                9300); // TCP 端口 9300，不是 HTTP 端口 9200

        Settings settings = Settings.builder()
                .put("cluster.name", "zhishen")
                .build();
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(node); // 可以添加多个节点
        log.info("当前连接的客户端信息为 {} ",client);
        return client;
    }

}
