package day4;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * 每隔30秒，统计最近30秒的数据 [滚动窗口]
 * 每隔30秒，统计最近1分钟的数据 [滑动窗口]
 */
public class Test1_Window {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop11:9092,hadoop12:9092,hadoop13:9092");
        properties.setProperty("group.id","g1");

        //使用flink消费kafka中topic1里面的数据
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("topic1",new SimpleStringSchema(),properties);
        DataStreamSource<String> ds1 = env.addSource(consumer);

        ds1.map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        return Tuple2.of(s,1);
                    }
                }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                        return tuple2.f0;   //华为5G
                    }
                })
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .window(SlidingProcessingTimeWindows.of(Time.minutes(1),Time.seconds(30)))
                .apply(new WindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {

                    @Override
                    public void apply(String key,    /*分组的key*/
                                      TimeWindow window,    /* 窗口对象 */
                                      Iterable<Tuple2<String, Integer>> input,  /*当前key在窗口中的所有数据*/
                                      Collector<String> out) throws Exception {  /* out用于输出  */
                        //窗口的开始时间,窗口的结束时间,热搜词汇,出现的次数
                        String s = "";
                        long start = window.getStart();
                        long end = window.getEnd();

                        String startTime = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss");
                        String endTime = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss");
                        int sum = 0;
                        for (Tuple2<String, Integer> tuple2 : input) {
                            sum += 1;
                        }
                        s = startTime + "," + endTime + "," + key + "," + sum;
                        out.collect(s);
                    }

                }).print();

        env.execute();
    }


}