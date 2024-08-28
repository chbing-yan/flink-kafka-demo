import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
//节点传输
//类型解析
//sink优化
public class DataCleanProducer {
    private static String string=" sssssssssssssssssssssssssssssssssssssssssssss ";
    private static String time1 = "2024年5月8日 12时12分12秒12233";
    private static String time2 = "2024-5-8 12-12-12 12";
    private static String time3 = "2024 5 8 12 12 12 000";
    private static String time4 = "20240508121201123";

    private static Integer int1=123;
    private static String double1=".45001";


    private static Boolean boolean1=true;
    public static void main(String[] args) throws Exception {

        // 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        // 创建一个简单的数据流
        DataStream<String> stream = env.addSource(new SourceFunction<String>() {
            private volatile boolean isRunning = true;
            @Override
            public void run(SourceContext<String> ctx) {
                int count=10000;
                while (isRunning && count>0) {
                    ctx.collect(generateString());
//                    count--;
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });


        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("124.221.14.39:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("before_clean")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // 将数据流写入 Kafka
        stream.sinkTo(sink);

        // 执行 Flink 任务
        env.execute("Flink Kafka Producer Example");
    }
    public static String generateString(){
        JSONObject jsonObject=new JSONObject();
        jsonObject.put("string1",string);
        jsonObject.put("string2",string);
        jsonObject.put("string3",string);
        jsonObject.put("string4",string);
        jsonObject.put("string5",string);
        jsonObject.put("string6",string);
        jsonObject.put("string7",string);
        jsonObject.put("string8",string);
        jsonObject.put("string9",string);
        jsonObject.put("string10",string);
        jsonObject.put("string11",string);
        jsonObject.put("string12",string);

        jsonObject.put("time01",time1);
        jsonObject.put("time02",time2);
        jsonObject.put("time03",time3);
        jsonObject.put("time04",time4);

        jsonObject.put("time11",time1);
        jsonObject.put("time12",time2);
        jsonObject.put("time13",time3);
        jsonObject.put("time14",time4);

        jsonObject.put("time21",time1);
        jsonObject.put("time22",time2);
        jsonObject.put("time23",time3);
        jsonObject.put("time24",time4);


        jsonObject.put("int1",int1);
        jsonObject.put("int2",int1);
        jsonObject.put("int3",int1);
        jsonObject.put("int4",int1);
        jsonObject.put("int5",int1);
        jsonObject.put("int6",int1);
        jsonObject.put("int7",int1);
        jsonObject.put("int8",int1);
        jsonObject.put("int9",int1);
        jsonObject.put("int10",int1);
        jsonObject.put("int11",int1);
        jsonObject.put("int12",int1);

        jsonObject.put("double1",double1);
        jsonObject.put("double2",double1);
        jsonObject.put("double3",double1);
        jsonObject.put("double4",double1);
        jsonObject.put("double5",double1);
        jsonObject.put("double6",double1);
        jsonObject.put("double7",double1);
        jsonObject.put("double8",double1);
        jsonObject.put("double9",double1);
        jsonObject.put("double10",double1);
        jsonObject.put("double11",double1);
        jsonObject.put("double12",double1);

        jsonObject.put("boolean1",boolean1);
        jsonObject.put("boolean2",boolean1);
        jsonObject.put("boolean3",boolean1);
        jsonObject.put("boolean4",boolean1);
        jsonObject.put("boolean5",boolean1);
        jsonObject.put("boolean6",boolean1);
        jsonObject.put("boolean7",boolean1);
        jsonObject.put("boolean8",boolean1);
        jsonObject.put("boolean9",boolean1);
        jsonObject.put("boolean10",boolean1);
        jsonObject.put("boolean11",boolean1);
        jsonObject.put("boolean12",boolean1);

        return jsonObject.toString();
    }
}

