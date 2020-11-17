import com.alibaba.fastjson.JSON;
import com.luwei.elasticsearchlearn.Application;
import com.luwei.elasticsearchlearn.entity.User;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class ElasticSearchDocTest {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    //es文档操作

    //测试添加文档
    @Test
    public void testAddDocument() throws IOException {
        //创建对象
        User user = new User("李四",32);
        //创建请求
        IndexRequest request = new IndexRequest("lu_index");

        //设置规则 put /lu_index/_doc/1
        request.id("2");
        request.timeout(TimeValue.timeValueSeconds(1));

        //将我们得数据放入请求 json
        request.source(JSON.toJSONString(user), XContentType.JSON);

        //客户端发送请求,获取相应的结果
        IndexResponse index = restHighLevelClient.index(request, RequestOptions.DEFAULT);

        System.out.println(index.toString());
        System.out.println(index.status());
    }
    
    //获取文档，判断是否存在 get /index/doc/1
    @Test
    public void testIsExits() throws IOException {
        GetRequest getRequest = new GetRequest("lu_index","1");
        //不获取返回得  _source 得上下文 效率更高
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        //判断是否存在
        boolean exists = restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //获取文档信息
    @Test
    public void testGetDoc() throws IOException {
        GetRequest getRequest = new GetRequest("lu_index","1");

        //判断是否存在
        GetResponse response = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(response.getSource());
    }

    //更新文档信息
    @Test
    public void testUpdateDoc() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("lu_index","1");
        User user = new User("王五",18);
        updateRequest.doc(JSON.toJSONString(user),XContentType.JSON);
        //判断是否存在
        UpdateResponse response = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    //删除文档记录
    @Test
    public void testDelDoc() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("lu_index","1");
        deleteRequest.timeout("1s");

        DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest,RequestOptions.DEFAULT);
        System.out.println(deleteResponse.status());
    }

    //批量存数据
    @Test
    public void testBulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("1s");

        ArrayList<User> userArrayList = new ArrayList<>();
        userArrayList.add(new User("he1",2));
        userArrayList.add(new User("he2",3));
        userArrayList.add(new User("he3",4));
        userArrayList.add(new User("he4",5));

        for(int i = 0; i < userArrayList.size(); i++){
            bulkRequest.add(
                    new IndexRequest("lu_index")
                    .id(""+i)
                    .source(JSON.toJSONString(userArrayList.get(i)),XContentType.JSON));
            BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            System.out.println(bulk.hasFailures());
        }
    }

    //查询
    @Test
    public void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("lu_index");

        //构建搜索条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //查询条件，我们可以使用QueryBuilders工具类构造查询条件
        //QueryBuilders.termQuery 精确匹配
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name","李四");
        sourceBuilder.query(termQueryBuilder);
        sourceBuilder.from();//分页
        sourceBuilder.size();
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(searchResponse.getHits()));
    }
}
