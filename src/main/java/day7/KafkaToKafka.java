package day7;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KafkaToKafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        //id,car,speed
        //1,豫A7718,65 {"id":"1","car":"豫A7718","speed":"65"}
        //2,豫A7720,65 {"id":"2","car":"豫A7718","speed":"65"}
        TableResult table1 = tenv.executeSql(
                "CREATE TABLE table1 (\n" +
                        "  id string,\n" +
                        "  car string,\n" +
                        "  speed STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'topic-car',\n" +
                        "  'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',\n" +
                        "  'properties.group.id' = 'testGroup',\n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'format' = 'json'\n" +
                        ")"
        );
        tenv.executeSql("CREATE TABLE table2 (\n" +
                "  id string,\n" +
                "  car string,\n" +
                "  speed STRING\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://localhost:3306/test',\n" +
                "   'table-name' = 't_violation_list',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456'\n" +
                ")");
        tenv.executeSql("insert into table2 select * from table1 where speed>60");

    }
}
