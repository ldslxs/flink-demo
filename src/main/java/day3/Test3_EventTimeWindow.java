package day3;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Test3_EventTimeWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //设置并行度为1

        /**
         * id,uid,money,eventtime
         * 1,10,50,2023-08-31 15:05:12
         * 2,10,30,2023-08-31 15:06:12
         * 3,10,10,2023-08-31 15:08:12
         * 4,10,40,2023-08-31 15:11:12
         * 5,10,20,2023-08-31 15:07:12
         * 4,10,40,2023-08-31 15:22:12
         */
        DataStreamSource<String> ds = env.socketTextStream("node01", 8889);
        ds.print();
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        private String  id;
        private Integer userId;
        private Integer money;
        private Long    createTime;
    }

}