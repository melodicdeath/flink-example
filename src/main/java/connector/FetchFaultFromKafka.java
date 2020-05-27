package connector;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * @author You
 * @Description
 * @create 2020-05-13 23:19
 **/
public class FetchFaultFromKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /* 初始化 Consumer 配置 */
        Properties consumerConfig = new Properties();
        consumerConfig.setProperty("bootstrap.servers", "10.3.161.88:9092;10.3.161.90:9092;10.3.161.91:9092");
        consumerConfig.setProperty("group.id", "xny-tbox-dispose3");
        consumerConfig.put("enable.auto.commit", "true");
        consumerConfig.put("auto.commit.interval.ms", "5000");

        /** 初始化 Kafka Consumer */
        FlinkKafkaConsumer<String> flinkKafkaConsumer =
                new FlinkKafkaConsumer<String>(
                        "xny_07",
                        new SimpleStringSchema(),
                        consumerConfig
                );
        flinkKafkaConsumer.setStartFromEarliest();
        /** 将 Kafka Consumer 加入到流处理 */
        DataStream<String> stream = env.addSource(flinkKafkaConsumer).setParallelism(2);
        stream.map((MapFunction<String, String>) s -> {
            JSONObject jsonObject = JSON.parseObject(s);
            if (jsonObject.getIntValue("kcdcnzzgzzs") > 0 || jsonObject.getIntValue("qddjgzzs") > 0
                    || jsonObject.getIntValue("fdjgzzs") > 0 || jsonObject.getIntValue("qtgzzs") > 0) {
                return s;
            }

            if(LocalDateTime.now().getMinute() % 5 == 0 && LocalDateTime.now().getSecond() == 0){
                return LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(jsonObject.getLong("sj")),
                        ZoneId.systemDefault())
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            }

            return null;
        }).filter((FilterFunction<String>) s -> {
            if(s != null) {
                return true;
            }

            return false;
        }).print();
        env.execute("Fetch Fault Data from topic xny_07");
    }
}
