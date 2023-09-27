package day5;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> ds = env.socketTextStream("node01", 8889);
        ds.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] str = s.split(",");
                return Tuple2.of(str[0],Integer.parseInt(str[1]));
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
                    @Override
                    public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                })
                .map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            ValueState<Integer> state;
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Integer>valueStateDescriptor=new ValueStateDescriptor<Integer>("maxValue",Integer.class);
                state = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                Integer i = state.value();
                if(i==null||i<stringIntegerTuple2.f1){
                    System.out.println(i);
                    state.update(stringIntegerTuple2.f1);
                }
                return Tuple2.of(stringIntegerTuple2.f0,state.value());
            }
        }).print();
        env.execute();
    }
}
