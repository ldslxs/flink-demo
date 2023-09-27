package day6;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class Test2_Checkpont {

    public static void main(String[] args) throws Exception {
        //ParameterTool tool = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.socketTextStream("node01", 8889);   //nc -lk 8888

        //1.开启checkpoint,每隔1s进行一次状态的快照
        env.enableCheckpointing(5000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.of(5,TimeUnit.SECONDS)));
        env.setStateBackend(new FsStateBackend("hdfs://node02:9000/flink-savepoint"));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        ds.map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        String[] arr = s.split(",");
                        return Tuple2.of(arr[0],Integer.parseInt(arr[1]));
                    }
                })
                .keyBy(v -> v.f0)
                .map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String,Integer>>() {

                    ValueState<Integer> valueState;  //数据存储在内存

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //1.创建一个托管状态下的键控状态下的ValueState
                        ValueStateDescriptor<Integer> tValueStateDescriptor = new ValueStateDescriptor<Integer>("vs1",Integer.class);
                        valueState = getRuntimeContext().getState(tValueStateDescriptor);
                    }

                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> t2) throws Exception {
                        System.out.println( " key = " + t2.f0 + ", 当前的状态值 = " + valueState.value());
                        //valueState.value()  获取状态
                        //valueState.update(v); 更新状态
                        Integer i = valueState.value();      //如果当前key是第一次出现，则获取的状态值是null
                        if(i == null){
                            valueState.update(t2.f1);
                        }else{
                            if(i < t2.f1){
                                valueState.update(t2.f1);
                            }
                        }
                        return Tuple2.of(t2.f0,valueState.value());
                    }

                }).print();


        env.execute();
    }

}
