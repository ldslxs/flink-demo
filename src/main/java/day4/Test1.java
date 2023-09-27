package day4;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;

public class Test1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> ds = env.socketTextStream("node01", 8889);
        OutputTag outputTag=new OutputTag<>("last", TypeInformation.of(AvgSpeed.class));
        ds.map(new MapFunction<String, CartInfo>() {
                    @Override
                    public CartInfo map(String s) throws Exception {
                        JSONObject json = JSON.parseObject(s);
                        return new CartInfo(json.getBigInteger("action_time"), json.getString("monitor_id"),
                                json.getString("camera_id"), json.getString("car"),
                                json.getDouble("speed"), json.getString("road_id"),
                                json.getString("area_id"));
                    }
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<CartInfo>forBoundedOutOfOrderness(Duration.ofMinutes(0)).withTimestampAssigner(new SerializableTimestampAssigner<CartInfo>() {
                    @Override
                    public long extractTimestamp(CartInfo cartInfo, long l) {
                        return new Long(String.valueOf(cartInfo.getAction_time()))*1000;
                    }
                }))
                .keyBy(new KeySelector<CartInfo, String>() {
                    @Override
                    public String getKey(CartInfo cartInfo) throws Exception {
                        return cartInfo.getMonitor_id();
                    }
                }).window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(5)))
                .apply(new WindowFunction<CartInfo, AvgSpeed, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<CartInfo> iterable, Collector<AvgSpeed> collector) throws Exception {
                        double speedAvg = 0;
                        int carAvg = 0;
                        String monitor_id ="";
                        for (CartInfo car : iterable) {
                            speedAvg+=car.getSpeed();
                            monitor_id=car.getMonitor_id();
                            carAvg+=1;
                        }
                        BigInteger start = BigInteger.valueOf(timeWindow.getStart());
                        BigInteger end = BigInteger.valueOf(timeWindow.getEnd());
                        double avgSpeed = speedAvg / carAvg;

                        collector.collect(new AvgSpeed(start,end,monitor_id,avgSpeed,carAvg));
                    }
                }).addSink(new RichSinkFunction<AvgSpeed>() {
                    Connection conn;
                    PreparedStatement ps;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","123456");
                        ps = conn.prepareStatement("insert into t_average_speed (id,start_time,end_time,monitor_id,avg_speed,car_count)values (null,?,?,?,?,?)");
                    }
                    @Override
                    public void invoke(AvgSpeed value, Context context) throws Exception {
                       ps.setString(1,String.valueOf(value.getStart_time()));
                       ps.setInt(2, value.getEnd_time().intValue());
                       ps.setString(3,value.getMonitor_id());
                       ps.setDouble(4,value.getAvg_speed());
                       ps.setInt(5, value.getCar_count());
                       ps.executeUpdate();

                    }

                    @Override
                    public void close() throws Exception {
                        ps.close();
                        conn.close();
                    }
                });


        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CartInfo {
        private BigInteger action_time;
        private String monitor_id;
        private String camera_id;
        private String car;
        private Double speed;
        private String road_id;
        private String area_id;
    }
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class AvgSpeed {
        private BigInteger start_time;
        private BigInteger end_time;
        private String monitor_id;
        private Double avg_speed;
        private Integer car_count;

    }
}
