package day6;

import avro.shaded.com.google.common.cache.*;
import com.alibaba.fastjson.JSON;
import day4.Test1;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

public class Test3 {

    public static void main(String[] args) throws Exception {
        //ParameterTool tool = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds = env.socketTextStream("node01", 8889);   //nc -lk 8888

        ds.map(new MapFunction<String, CartInfo>() {
            @Override
            public CartInfo map(String s) throws Exception {
                CartInfo cartInfo = JSON.parseObject(s, CartInfo.class);
                return cartInfo;
            }
        }).flatMap(new RichFlatMapFunction<CartInfo, CartInfo2>() {
            Connection conn;
            PreparedStatement ps;
            ResultSet res;
            LoadingCache<String, MonitorInfo> cache;
            @Override
            public void open(Configuration parameters) throws Exception {
                conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456");
                ps = conn.prepareStatement("select *from t_monitor_info where monitor_id=?");
                cache = CacheBuilder.newBuilder().maximumSize(1000)
                        .expireAfterWrite(10, TimeUnit.SECONDS)
                        .removalListener(new RemovalListener<String, MonitorInfo>() {
                            @Override
                            public void onRemoval(RemovalNotification<String, MonitorInfo> removalNotification) {
                                System.out.println(removalNotification.getKey() + "被移除了，值为：" + removalNotification.getValue());
                            }
                        }).build(new CacheLoader<String, MonitorInfo>() {
                            @Override
                            public MonitorInfo load(String s) throws Exception {
                                System.out.println(" s  = " + s);
                                ps.setString(1, s);
                                res = ps.executeQuery();
                                String monitor_id = "无";
                                MonitorInfo monitorInfo = new MonitorInfo();
                                while (res.next()) {
                                    monitorInfo.setMonitor_id(res.getString("monitor_id"));
                                    monitorInfo.setRoad_id(res.getString("road_id"));
                                    monitorInfo.setSpeed(res.getDouble("speed_limit"));
                                    monitorInfo.setArea_id(res.getString("area_id"));
                                }
                                return monitorInfo;
                            }
                        });

            }

            @Override
            public void flatMap(CartInfo cartInfo, Collector<CartInfo2> collector) throws Exception {
                MonitorInfo monitorInfo = cache.get(cartInfo.getMonitor_id());
                System.out.println(monitorInfo.getSpeed() * 1.1);
                System.out.println(cartInfo.getSpeed());
                if (monitorInfo.getSpeed() * 1.1 < cartInfo.getSpeed()) {
                    System.out.println(123456);
                    CartInfo2 cartInfo2 = new CartInfo2();
                    cartInfo2.setId(0);
                    cartInfo2.setMonitor_id(cartInfo.getMonitor_id());
                    cartInfo2.setCar(cartInfo.getCar());
                    cartInfo2.setRoad_id(cartInfo.getRoad_id());
                    cartInfo2.setActionTime(BigInteger.valueOf(cartInfo.getAction_time()));
                    cartInfo2.setLimitSpeed(monitorInfo.getSpeed().intValue());
                    cartInfo2.setRealSpeed(cartInfo.getSpeed());
                    collector.collect(cartInfo2);
                    for (int i = 0; i <0 ; i++) {
                        System.out.println(1);
                    }
                }

            }

            @Override
            public void close() throws Exception {
                ps.close();
                conn.close();
                res.close();
            }
        }).addSink(new RichSinkFunction<CartInfo2>() {
            Connection conn;
            PreparedStatement ps;
            @Override
            public void open(Configuration parameters) throws Exception {
                conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456");
                ps = conn.prepareStatement("insert into t_speeding_info values (null,?,?,?,?,?,?)");

            }

            @Override
            public void invoke(CartInfo2 value, Context context) throws Exception {
                ps.setString(1,value.getCar());
                ps.setString(2,value.getMonitor_id());
                ps.setString(3,value.getRoad_id());
                ps.setDouble(4,value.getRealSpeed());
                ps.setInt(5,value.getLimitSpeed());
                ps.setInt(6,value.getActionTime().intValue());
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
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CartInfo {
        private long action_time;
        private String monitor_id;
        private String camera_id;
        private String car;
        private Double speed;
        private String road_id;
        private String area_id;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CartInfo2 {
        private int id;
        private String car;
        private String monitor_id;
        private String road_id;
        private Double realSpeed;
        private int limitSpeed;
        private BigInteger actionTime;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MonitorInfo {
        private String monitor_id;
        private String road_id;
        private Double speed;
        private String area_id;
    }
}
