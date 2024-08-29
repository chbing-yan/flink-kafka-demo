import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.regex.Pattern;

public class DataCleanConsumer {
    static ParameterTool parameterTool;

    static {
        try {
//            String projectDir = System.getProperty("user.dir");
//            System.out.println(projectDir);
            parameterTool = ParameterTool.fromPropertiesFile(new FileInputStream("src/main/resources/config.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration configuration=new Configuration();
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.disableOperatorChaining();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
//                .setBootstrapServers("124.221.14.39:9092")
                .setBootstrapServers("localhost:9092")
                .setTopics("before_clean")
                .setGroupId("clean_app")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();


        DataStream<String> inputStream=env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStream<String> cleaned_data=inputStream
                .map(DataCleanConsumer::clean); // 简单的字符串前缀

        KafkaSink<String> sink = KafkaSink.<String>builder()
//                .setBootstrapServers("124.221.14.39:9092")
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("after_clean")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // 将数据流写入 Kafka
        cleaned_data.sinkTo(sink);

        // 执行 Flink 任务
        env.execute("Flink Kafka Producer Example");

    }
    public static String clean(String input) throws InterruptedException {
        JSONObject jsonObject = customParse(input);
        if(jsonObject==null){
            return "";
        }
        JSONObject cleanedObject=new JSONObject();
        jsonObject.forEach((key, value) -> {
            String stringValue=(String) value;
            String rule=parameterTool.get(key);
//            System.out.println(key);
            System.out.println(rule);
            if(rule==null){
                cleanedObject.put(key,stringValue);
                return;
            }
            switch (rule){


                case "stringDefault":{
                    cleanedObject.put(key,cleanString(stringValue));
                    return;
                }
                case "stringToTimestamp":{
                    cleanedObject.put(key,cleanTimestamp(stringValue));
                    return;
                }
                case "intDefault":{
                    cleanedObject.put(key,cleanInt(stringValue));
                    return;
                }
                case "doubleDefault":{
                    cleanedObject.put(key,cleanDouble(stringValue));
                    return;
                }
                case "booleanDefault":{
                    cleanedObject.put(key,cleanBoolean(stringValue));
                    return;
                }
                default:{
                    cleanedObject.put(key,stringValue);
                }
            }
        });
//        Thread.sleep(1000);

        return cleanedObject.toJSONString();
    }
    public static String cleanString(String string){
        if(string==null||"null".equalsIgnoreCase(string)){
            return "null";
        }
        return string.trim();
    }
    public static String cleanTimestamp(String string){
        String ans;
        if(Pattern.matches("\\d{14,}",string)){
            String ms= StringUtils.rightPad(string.substring(14,17),3,'0');
            ans=String.format("%4s-%02d-%02dT%02d:%02d:%02d.%3s",string.substring(0,4),
                    Integer.valueOf(string.substring(4,6)),
                    Integer.valueOf(string.substring(6,8)),
                    Integer.valueOf(string.substring(8,10)),
                    Integer.valueOf(string.substring(10,12)),
                    Integer.valueOf(string.substring(12,14)),
                    ms);
        }
        else {


//        String pattern = "\\d4\\D\\d{1,2}\\D\\d{1,2}\\D\\d{1,2}\\D\\d{1,2}\\D\\d{1,2}\\D\\d+";
//
//        boolean isMatch = Pattern.matches(pattern, string);
//        if(Pattern.matches())
            String[] strings = string.split("\\D+");
            String ms=StringUtils.rightPad(strings[6],3,'0').substring(0,3);
            ans = String.format("%4s-%02d-%02dT%02d:%02d:%02d.%3s", strings[0],
                    Integer.valueOf(strings[1]),
                    Integer.valueOf(strings[2]),
                    Integer.valueOf(strings[3]),
                    Integer.valueOf(strings[4]),
                    Integer.valueOf(strings[5]),
                    ms);
        }
//        System.out.println(isMatch);
        return ans;
    }
    public static String cleanInt(String string){
        return string;
    }
    public static String cleanDouble(String string){
        return String.format("%.2f",Double.valueOf(string));
    }
    public static String cleanBoolean(String string){
        return string;
    }
    public static JSONObject customParse(String jsonString) {
        // 创建 JSONReader 对象
        JSONReader reader = new JSONReader(new StringReader(jsonString));
        try {
            reader.startObject();
        }
        catch (Exception e){
            return null;
        }
        // 开始解析对象


        // 创建存储解析结果的 JSONObject
        JSONObject jsonObject = new JSONObject();

        while (reader.hasNext()) {
            String key = reader.readString(); // 读取键
            String value = reader.readString(); // 读取值
            jsonObject.put(key, value); // 将值转换为字符串并放入 JSONObject
        }

        reader.endObject(); // 结束对象解析
        try {
            reader.close(); // 关闭 JSONReader
        }catch (Exception e){
            return null;
        }


        return jsonObject;
    }
}
