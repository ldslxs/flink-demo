package day7;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


public class TumblingWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds1 = env.socketTextStream("node01", 8889);   //nc -lk 8888
        DataStreamSource<String> ds2 = env.socketTextStream("node01", 8899);   //nc -lk 8888
        SingleOutputStreamOperator<Tuple3<String,String,Long>> ds3 =  ds1.map(new MapFunction<String, Tuple3<String,String,Long>>() {

            @Override
            public Tuple3<String, String, Long> map(String s) throws Exception {
                String[] arr = s.split(",");
                long time = DateUtils.parseDate(arr[2], "yyyy-MM-dd HH:mm:ss").getTime();
                return Tuple3.of(arr[0], arr[1], time);
            }
        });
        SingleOutputStreamOperator<Tuple3<String,String,Long>> ds4 = ds2.map(new MapFunction<String, Tuple3<String,String,Long>>() {
            @Override
            //key,1,2020-01-18 10:34:33
            //key,2,2020-01-18 10:34:33
            public Tuple3<String,String,Long> map(String s) throws Exception {
                String[] arr = s.split(",");
                long time = DateUtils.parseDate(arr[2], "yyyy-MM-dd HH:mm:ss").getTime();
                return  Tuple3.of(arr[0], arr[1], time);
            }
        });
        ds3.join(ds4).where(tuple3 -> tuple3.f0).equalTo(tuple3 -> tuple3.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .apply(new JoinFunction<Tuple3<String,String,Long>, Tuple3<String,String,Long>, Tuple3<String,String,String>>() {
                    @Override
                    public Tuple3<String,String,String> join(Tuple3<String,String,Long> tuple3, Tuple3<String,String,Long> tuple32) throws Exception {

                        return Tuple3.of(tuple3.f0,tuple3.f1,tuple32.f1);
                    }
                }).print();
        env.execute();
    }
}
