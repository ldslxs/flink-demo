package day5;

import day4.Test1;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

public class Test3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "node01:9092");
        props.setProperty("group.id", "g1");
        FlinkKafkaConsumer<String> consumer=new FlinkKafkaConsumer<String>("topic-car", new SimpleStringSchema(), props);
        env.setParallelism(1);
        DataStreamSource<String> ds = env.addSource(consumer);
        ds.map(new MapFunction<String, ViolateCar>() {
            @Override
            public ViolateCar map(String s) throws Exception {
                String[] arr = s.split(",");
                return new ViolateCar(Integer.parseInt(arr[0]),arr[1],arr[2],new BigInteger(arr[3]));
            }
        }).keyBy(new KeySelector<ViolateCar, Object>() {
            @Override
            public Object getKey(ViolateCar violateCar) throws Exception {
                return violateCar.getCar();
            }
        }).flatMap(new RichFlatMapFunction<ViolateCar, ViolateCar>() {
            ValueState<BigInteger> state;
           // ListState<ViolateCar> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<BigInteger> valueStateDescriptor = new ValueStateDescriptor<BigInteger>("maxValue", BigInteger.class);
                state = getRuntimeContext().getState(valueStateDescriptor);
               // ListStateDescriptor<ViolateCar> listStateDescriptor = new ListStateDescriptor<ViolateCar>("list1", ViolateCar.class);
               // listState = getRuntimeContext().getListState(listStateDescriptor);
            }

            @Override
            public void flatMap(ViolateCar violateCar, Collector<ViolateCar> collector) throws Exception {
                if(state.value()==null){
                    state.update(violateCar.getCreatTime());
                }else if(Math.abs(Integer.parseInt(String.valueOf(state.value()))-
                        Integer.parseInt(String.valueOf(violateCar.getCreatTime())))<=5){
                   // listState.add(violateCar);

                    state.update(violateCar.getCreatTime());
                    collector.collect(violateCar);
                }else {
                    state.update(violateCar.getCreatTime());
                }
            }
        }).addSink(new RichSinkFunction<ViolateCar>() {
            Connection conn;
            PreparedStatement ps;
            @Override
            public void open(Configuration parameters) throws Exception {
                conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","123456");
                ps = conn.prepareStatement("insert into t_violation_list (id,car,violation,create_time)values (null,?,?,?)");
            }
            @Override
            public void invoke(ViolateCar value, Context context) throws Exception {
                ps.setString(1,String.valueOf(value.getCar()));
                ps.setString(2,value.getViolation());
                ps.setInt(3,value.getCreatTime().intValue());

                ps.executeUpdate();

            }

            @Override
            public void close() throws Exception {
                ps.close();
                conn.close();
            }
        });
        //1,豫A747778,套牌,1693575555



        env.execute();
    }
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ViolateCar{
        private Integer id;
        private String car;
        private String violation;
        private BigInteger creatTime;
    }
}
