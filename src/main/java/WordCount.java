import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
public class WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment exec = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds = exec.fromElements("asdsa hhh hhh www ", "hhh li wangwu", "hhh eee");
        SingleOutputStreamOperator<String> ds2 = ds.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] s1 = s.split(" ");
                for (String s2 : s1
                ) {
                    collector.collect(s2);
                }
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds3 = ds2.map(
          (String s)-> {
                return new Tuple2<>(s, 1);
            }
        ).returns(Types.TUPLE(Types.STRING,Types.INT));
        exec.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        ds3.keyBy(
            (Tuple2<String, Integer> stringIntegerTuple2)-> {
                return stringIntegerTuple2.f0;
            }
        ).sum(1).print();

        exec.execute();
    }
}
