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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;


/**
 * 1,lisi,2023-01-22 10:25:22,10
 * 2,lisi,2023-01-22 10:22:22,20
 * 1,lisi,2023-01-22 10:26:22,30
 * 4,lisi,2023-01-22 10:30:22,40
 * 1,lisi,2023-01-22 10:35:22,40
 * 1,lisi,2023-01-22 10:40:22,40
 */
public class TestSink2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds1 = env.socketTextStream("node01", 8889);
        env.setParallelism(1);
        KeyedStream<Student, Integer> ds2 = ds1.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String s) throws Exception {
                String[] str = s.split(",");
                long time = DateUtils.parseDate(str[2], "yyyy-MM-dd HH:mm:ss").getTime();
                return new Student(Integer.parseInt(str[0]), str[1], time, Long.parseLong(str[3]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Student>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<Student>() {
                    @Override
                    public long extractTimestamp(Student student, long l) {
                        return student.getDate();
                    }
                })).keyBy(new KeySelector<Student, Integer>() {
            @Override
            public Integer getKey(Student student) throws Exception {
                return student.getId();
            }
        });
         //ds2.sum("age").print();
        ds2.window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .apply(new WindowFunction<Student, String, Integer, TimeWindow>() {
                    @Override
                    public void apply(Integer integer, TimeWindow timeWindow, Iterable<Student> iterable, Collector<String> collector) throws Exception {
                        long start = timeWindow.getStart();
                        long end = timeWindow.getEnd();
                        long sum = 0;
                        for (Student stu : iterable) {
                            sum += stu.getAge();

                        }
                        String s = "";
                        String start1 = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss");
                        String end1 = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss");
                        s = integer + "," + sum + "," + start1 + "," + end1;
                        collector.collect(s);
                    }
                });
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Student {
        private Integer id;
        private String name;
        private long date;
        private long age;

    }
}
