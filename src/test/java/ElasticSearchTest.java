import com.luwei.elasticsearchlearn.Application;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.io.IOException;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class ElasticSearchTest {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    //测试创建索引
    @Test
    public void testCreateIndex() throws IOException {
        //1、创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("lu_index");
        //2、执行请求
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse.index());
    }

    //测试获取索引
    @Test
    public void testExistIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("lu_index");
        boolean isExist = restHighLevelClient.indices().exists(request,RequestOptions.DEFAULT);
        System.out.println(isExist);
    }
    //测试删除索引
    @Test
    public void deleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("lu_index");
        AcknowledgedResponse response = restHighLevelClient.indices().delete(request,RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }
}
