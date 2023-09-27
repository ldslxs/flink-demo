
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

public class FlinkKafkaConn {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "node01:9092");
        props.setProperty("group.id", "g1");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("topic-car", new SimpleStringSchema(), props);
        DataStreamSource<String> ds = env.addSource(consumer);

        SingleOutputStreamOperator<CartInfo> ds2 = ds.map(new MapFunction<String, CartInfo>() {
            @Override
            public CartInfo map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                return new CartInfo(jsonObject.getInteger("action_time"),
                        jsonObject.getString("monitor_id"),
                        jsonObject.getString("camera_id"),
                        jsonObject.getString("car"),
                        jsonObject.getDouble("speed"),
                        jsonObject.getString("road_id"),
                        jsonObject.getString("area_id"));
            }
        });
        ds2.filter(new FilterFunction<CartInfo>() {
            @Override
            public boolean filter(CartInfo cartInfo) throws Exception {
                return cartInfo.speed>60;
            }
        }).addSink(new RichSinkFunction<CartInfo>() {
            Connection conn;
            PreparedStatement ps;
            @Override
            public void open(Configuration parameters) throws Exception {
               conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","123456");
                ps = conn.prepareStatement("insert into t_monitor_info (monitor_id,road_id,speed_limit,area_id)values (?,?,?,?)");
            }

            @Override
            public void invoke(CartInfo value, Context context) throws Exception {
                ps.setString(1,value.getMonitor_id());
                ps.setString(2,value.getRoad_id());
                ps.setInt(3, (int) value.getSpeed());
                ps.setString(4,value.getArea_id());
                ps.executeUpdate();
            }
        });

        env.execute();
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CartInfo{
        private Integer action_time;
        private String monitor_id;
        private String camera_id;
        private String car;
        private double speed;
        private String road_id;
        private String area_id;

    }
}
