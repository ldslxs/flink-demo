import javafx.beans.property.SimpleIntegerProperty;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class FlinkKafkaConn2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "node01:9092");
        props.setProperty("group.id", "g1");
        FlinkKafkaProducer<String> producer=new FlinkKafkaProducer<String>("t1", new SimpleStringSchema(), props);

       // FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("t1", new SimpleStringSchema(), props);
        env.setParallelism(2);


        DataStreamSource<String> ds1 = env.fromElements("zhangsan","lisi","wangwu","zhaoliu");
        DataStreamSource<Integer> ds2 = env.fromElements(1,2,3,4,5,6,7);
        ConnectedStreams<String, Integer> ds3 = ds1.connect(ds2);
        OutputTag<String>out1=new OutputTag<>("单词", TypeInformation.of(String.class));
        OutputTag<Integer>out2=new OutputTag<>("数字", TypeInformation.of(Integer.class));
        SingleOutputStreamOperator<Object> process = ds3.process(new CoProcessFunction<String, Integer, Object>() {
            @Override
            public void processElement1(String s, CoProcessFunction<String, Integer,
                    Object>.Context context, Collector<Object> collector) throws Exception {
                context.output(out1, s);
            }

            @Override
            public void processElement2(Integer integer, CoProcessFunction<String, Integer,
                    Object>.Context context, Collector<Object> collector) throws Exception {
                context.output(out2, integer);
            }
        });
        process.getSideOutput(out1).addSink(producer);
        process.getSideOutput(out2).map(new MapFunction<Integer, String>() {
            @Override
            public String map(Integer integer) throws Exception {
                return integer.toString();
            }
        }).addSink(producer);
        env.execute();
    }
}
