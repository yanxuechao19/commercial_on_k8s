package com.commercial;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import java.text.DecimalFormat;
import org.apache.http.HttpHost;
import org.elasticsearch.common.xcontent.XContentType;

/*
import org.apache.http.auth.UsernamePasswordCredentials;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.common.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.common.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.common.mapping.ElasticsearchMappingUtils;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.core5.http.HttpHost;
import org.opensearch.client.RestHighLevelClient;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.aws.AwsSdk2Transport;
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import com.amazonaws.http.apache.client.impl.SdkHttpClient;
import org.elasticsearch.action.index.IndexRequest;*/




/**
 * packageName com.commercial.balance_demo
 *
 * @author yanxuechao
 * @version JDK 8
 * @className CommercialReduce
 * @date 2024/7/2
 * @description TODO
 */
public class CommercialReduce {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // TODO 1. 读取业务主流
        String topic = "growalong_prod";
        String groupId = "growalong_prod_0702";
        DataStreamSource<String> coinDs = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));
       //DataStreamSource<String> coinDs = env.readTextFile("D:\\code\\commercial_on_ds\\input\\user_coin_log.txt");
        // TODO 2. 主流数据结构转换
        SingleOutputStreamOperator<JSONObject> jsonDS = coinDs.map( JSON::parseObject);
        // TODO 3. 主流 ETL
        SingleOutputStreamOperator<JSONObject> filterDS = jsonDS.filter(
                jsonObj ->
                {
                    try {

                        if(jsonObj.getString("table").equals("user_coin_wallet_log")  && jsonObj.getJSONObject("data").getString("change_type").equals("coin_recharge")) {
                            return true;
                        }
                        return false;
                    } catch (JSONException jsonException) {
                        return false;
                    }
                });
        //filterDS.print();
        // TODO 4. 主流获取需要的字段
        SingleOutputStreamOperator<Double> mapstram = filterDS.map(new MapFunction<JSONObject, Double>() {
            @Override
            public Double map(JSONObject value) throws Exception {
                return value.getJSONObject("data").getDouble("balance");
            }
        });
        // TODO 5. 对获取的字段进行聚合
        SingleOutputStreamOperator<Double> result = mapstram.keyBy(key -> true)
                .reduce(new RichReduceFunction<Double>() {
                    DecimalFormat df;
                    Double rs;


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        df = new DecimalFormat("0.0000");


                    }

                    @Override
                    public Double reduce(Double value1, Double value2) throws Exception {
                        rs = ((((value1 / 0.9) / 100 - ((value1 / 0.9) / 100 * 0.3 * 0.3)) - (((value1 / 0.9) / 100 - ((value1 / 0.9) / 100 * 0.3 * 0.3)) * 0.95)) - (value1 / 0.9) / 100 * 0.3 * (1 - 0.3)) + ((((value2 / 0.9) / 100 - ((value2 / 0.9) / 100 * 0.3 * 0.3)) - (((value2 / 0.9) / 100 - ((value2 / 0.9) / 100 * 0.3 * 0.3)) * 0.95)) - (value2 / 0.9) / 100 * 0.3 * (1 - 0.3));
                        String result = df.format(rs);
                        double v = Double.parseDouble(result);
                        return v;
                    }
                });
  

        HttpHost httpHosts = new HttpHost("vpc-prod-opensearch-e6zsd5jz7y24fgwlhws6gwofzm.us-east-1.es.amazonaws.com", 443, "https");
        XContentType xContentType = XContentType.JSON;
        boolean auth = true; // 假设需要认
        // 使用ElasticSearchSinkConfig.Builder来构建ElasticSearchSinkConfig实例
        ElasticSearchSinkConfig config = new ElasticSearchSinkConfig.Builder()
                .withHttpHost(httpHosts)
                .withIndexName("balance")
                .withXContentType(xContentType)
                .withUsername("prod-opensearch-syncDB-user")
                .withPassword("sFZNHvVq$vHpg8H9")
                .withAuth(auth)
                .build();
        OpenSearchSinkFactory openSearchSinkFactory = new OpenSearchSinkFactory();
        ElasticsearchSink<Double> openSearchSink = openSearchSinkFactory.createOpenSearchSink(config);

        result.addSink(openSearchSink);
        env.execute();
    }




}



