import com.alibaba.fastjson.JSON;
import com.luwei.elasticsearchlearn.Application;
import com.luwei.elasticsearchlearn.entity.User;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

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

}
