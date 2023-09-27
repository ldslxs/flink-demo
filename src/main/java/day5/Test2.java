package day5;

import akka.io.Inet;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class Test2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> ds = env.socketTextStream("node01", 8889);
        ds.map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        String[] str = s.split(",");
                        return Tuple2.of(str[0], Integer.parseInt(str[1]));
                    }
                }).keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
                    @Override
                    public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                })
                .flatMap(new RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, List<Integer>>>() {

                    ValueState<Integer> state;
                    ListState<Integer> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<Integer>("maxValue", Integer.class);
                        state = getRuntimeContext().getState(valueStateDescriptor);
                        ListStateDescriptor<Integer> listStateDescriptor = new ListStateDescriptor<Integer>("list1", Integer.class);
                        listState = getRuntimeContext().getListState(listStateDescriptor);
                    }

                    /**
                     * * 输入                      输出
                     * 许宁,37
                     * 许宁,38
                     * 许宁,39
                     * 许宁,35
                     * 许宁,40
                     * 许宁,41               许宁,[39,40,41]
                     * 许宁,40
                     */
                    @Override
                    public void flatMap(Tuple2<String, Integer> value, Collector<Tuple2<String, List<Integer>>> collector) throws Exception {
                        if (value.f1 > 38) {
                            int i = state.value() == null ? 1 : state.value() + 1;
                            state.update(i);
                            listState.add(value.f1);
                            if (state.value() >= 3) {
                                List<Integer> list  = new ArrayList<>();
                                for (Integer v1 : listState.get()) {
                                    list.add(v1);
                                }
                                collector.collect(Tuple2.of(value.f0,list));

                            }
                        }
                    }

                }).print();
        env.execute();
    }
}
