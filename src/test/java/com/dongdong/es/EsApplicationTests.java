package com.dongdong.es;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializeFilter;
import com.dongdong.es.config.ElasticSearchConfig;
import com.dongdong.es.pojo.User;
import net.minidev.json.JSONArray;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.JSONString;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

@SpringBootTest
class EsApplicationTests {

    @Autowired
    RestHighLevelClient restHighLevelClient;
    @Test
    void contextLoads() throws IOException {
        //创建索引请求
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("dongge");
        CreateIndexResponse createIndexResponse =
                restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

    @Test
    //获取索引
    void getIndex() throws IOException {
        //判断索引是否存在
        GetIndexRequest getIndexRequest = new GetIndexRequest("dongge");
        boolean exists = restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }
    @Test
    void delIndex() throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("dongge");
        AcknowledgedResponse response =
                restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    @Test
    //测试添加文档
    void addDocu() throws IOException {
        User user = new User("杨伟栋", 23);
        IndexRequest indexRequest = new IndexRequest("dongge");
        indexRequest.id("1");
        indexRequest.source(JSON.toJSONString(user), XContentType.JSON);
        //客户端发送请求
        IndexResponse indexResponse =
                restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse);
    }

    @Test
    //获得文档信息
    void getDocuInfo() throws IOException {
        GetRequest getRequest = new GetRequest("dongge", "1");

        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.isExists());
        Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
        System.out.println(getResponse.getSourceAsString());//打印文档内容
        System.out.println(sourceAsMap);

    }

    @Test
        //更新文档信息
    void updateDocuInfo() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("dongge", "1");

        User user = new User("dongdong111",24);
        updateRequest.doc(JSON.toJSONString(user),XContentType.JSON);
        restHighLevelClient.update(updateRequest,RequestOptions.DEFAULT);


    }


    @Test
        //删除文档信息
    void deleteDocuInfo() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("dongge", "1");

        restHighLevelClient.delete(deleteRequest,RequestOptions.DEFAULT);

    }

    @Test
        //批量插入数据
    void bluckDocuInfo() throws IOException {
        BulkRequest bulkRequest = new BulkRequest("dongge");

        ArrayList<User> userList = new ArrayList<>();
        userList.add(new User("dong1",12));
        userList.add(new User("dong2",13));
        userList.add(new User("dong3",14));
        userList.add(new User("dong4",18));
        userList.add(new User("dong5",1765));
        userList.add(new User("dong6",145));
        //批处理请求
        for (int i=0;i<userList.size();i++) {
            bulkRequest.add(
                    new IndexRequest()
                            .id(""+(i+1))
                            .source(JSON.toJSONString(userList.get(i)),XContentType.JSON)
            );
        }
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulkResponse.status());


    }

    //查询
    @Test
    void searchTest() throws IOException {
        SearchRequest searchRequest = new SearchRequest("dongge");
        //构建查询器
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        TermQueryBuilder queryBuilders =  QueryBuilders.termQuery("name","dong1");
        sourceBuilder.query(queryBuilders);
        searchRequest.source(sourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(search);
    }
}
