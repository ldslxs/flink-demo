package day7;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KafkaToMysql {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        TableResult table1 = tenv.executeSql(
                "CREATE TABLE table1 (\n" +
                        "  city string,\n" +
                        "  temperature string\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'topic1',\n" +
                        "  'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',\n" +
                        "  'properties.group.id' = 'testGroup',\n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'format' = 'json'\n" +
                        ")"
        );
        tenv.executeSql("CREATE TABLE table2 (\n" +
                "  city string PRIMARY KEY NOT ENFORCED,\n" +
                "  max_temperature string\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://localhost:3306/test',\n" +
                "   'table-name' = 'city_temperature',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456'\n" +
                ")");
        tenv.executeSql("insert into table2 select city ,max(temperature) from table1 group by city").print();

    }
}
