package com.springapp.mvc;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterIndexHealth;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

/**
 * Created by jenny on 15/4/22.
 */
@ContextConfiguration(locations = {"classpath:spring-context.xml"})
public class AppTestNG extends AbstractTestNGSpringContextTests {
    @Autowired
    private Client client;
    private ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testMapping() throws IOException {
        //{"mappings":{"user":{"properties":{"birthDay":{"type":"long"},"id":{"type":"long"},"name":{"type":"string"}}}}}
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject()
                .startObject("properties")
                .startObject("birthDay").field("type", "long").endObject()
                .startObject("id").field("type", "long").endObject()
                .startObject("name").field("type", "string").field("index", "not_analyzed").endObject()
                .endObject().endObject();
//        System.out.println(xContentBuilder);
        //判断index是否存在
        ClusterHealthRequest clusterHealthRequest = Requests.clusterHealthRequest("users");
        ClusterHealthResponse clusterHealthResponse = client.admin().cluster().health(clusterHealthRequest).actionGet();
        Map<String, ClusterIndexHealth> map = clusterHealthResponse.getIndices();
        if (!map.containsKey("users")) {
            CreateIndexResponse createIndexResponse = client.admin().indices()
                    .create(new CreateIndexRequest("users")).actionGet();
            System.out.println("create---" + createIndexResponse.isAcknowledged());
            //wait yellow
            client.admin().cluster().health(new ClusterHealthRequest("users").waitForYellowStatus()).actionGet();

//            IndexRequestBuilder indexQueryBuilder = client.prepareIndex("users", "user");
//            indexQueryBuilder.setOpType(IndexRequest.OpType.CREATE);
//            IndexResponse indexResponse = indexQueryBuilder.execute().actionGet();
//            if (indexResponse.isCreated()) {
//                System.out.println("index create success.");
//            } else {
//                System.out.println("index create error.");
//            }
        }
        PutMappingRequest putMappingRequest = Requests.putMappingRequest("users").type("user").source(xContentBuilder);
        PutMappingResponse putMappingResponse = client.admin().indices().putMapping(putMappingRequest).actionGet();
        System.out.println(putMappingResponse.isAcknowledged());

    }

    @Test
    public void bulkIndex() throws JsonProcessingException {
        List<String> list1 = new ArrayList<String>();
        list1.add("A");
        list1.add("a");
        list1.add("a23");
        list1.add("23");
        List<String> list2 = new ArrayList<String>();
        list2.add("B");
        list2.add("a");
        list2.add("44");
        list2.add("b4");
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for (int i = 0; i < 1000; i++) {
            User u = new User();
            u.setId(i);
            if (i % 3 == 0) {
                u.setName(i % 2 == 0 ? "tone & jenny" : "tom \\ jerry");
            }
            u.setBirthDay(new Date(System.currentTimeMillis() + (new Random().nextInt(24) * 24 * 3600 * 1000)));
            if (i % 3 == 0) {
                u.setBooks(i % 2 == 0 ? list1 : list2);
            }
            IndexRequestBuilder indexRequestBuilder = client.prepareIndex("users", "user", String.valueOf(i))
                    .setId(String.valueOf(i)).setSource(objectMapper.writeValueAsString(u));
            bulkRequestBuilder.add(indexRequestBuilder);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            System.out.println(bulkResponse.buildFailureMessage());
        } else {
            System.out.println("index with bulk success." + bulkResponse.getTookInMillis() + "ms");
        }
    }

    @Test
    public void allMatch() throws IOException {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("users", "user");
        searchRequestBuilder.setQuery(QueryBuilders.matchPhraseQuery("name", "tom"));
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        printHits(searchResponse.getHits());
    }

    private void printHits(SearchHits searchHits) throws IOException {
        for (SearchHit sh : searchHits) {
            System.out.println(objectMapper.readValue(sh.getSourceAsString(), User.class));
        }
    }

    @Test
    public void deleteAll() {
        DeleteIndexResponse deleteResponse = client.admin().indices().prepareDelete("users").execute().actionGet();
        if (deleteResponse.isAcknowledged()) {
            System.out.println("acknowledged");
        } else {
            System.out.println("oo ");
        }

    }

    @Test
    public void testIndex() throws JsonProcessingException {
        for (int i = 0; i < 10; i++) {
            User u = new User();
            u.setId(i);
            u.setName("tone " + i);
            u.setBirthDay(new Date());
//            u.setTime();
            IndexRequestBuilder indexRequestBuilder = client.prepareIndex("users", "user");
            indexRequestBuilder.setId(String.valueOf(i)).setOpType(IndexRequest.OpType.INDEX)
                    .setSource(objectMapper.writeValueAsString(u));
            IndexResponse indexResponse = indexRequestBuilder.execute().actionGet();
            if (indexResponse.isCreated()) {
                System.out.println(u.getId() + " created success.");
            } else {
                System.out.println(u.getId() + " created error.");
            }
        }
        client.close();

    }

    @Test
    public void testDeleteAll() {
        DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete();
        deleteRequestBuilder.setIndex("users");
        deleteRequestBuilder.setType("user");
        for (int i = 0; i < 10; i++) {
            deleteRequestBuilder.setId(String.valueOf(i));
            DeleteResponse deleteResponse = deleteRequestBuilder.execute().actionGet();
            if (deleteResponse.isFound()) {
                System.out.println("id:" + i + " delete all succee.");
            } else {
                System.out.println("id:" + i + " delete all failure.");
            }
        }
        client.close();
    }

    @Test
    public void testBoolQuery() {

        client.close();
    }

}
