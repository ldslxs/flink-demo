package day8;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Test_Topn {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.executeSql("CREATE TABLE table1 (" +
                "    `ts` TIMESTAMP(3),\n" +
                "    page_id INT,\n" +
                "    clicks INT,\n" +
                "  WATERMARK FOR ts AS ts - INTERVAL '10' SECOND \n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'topic1',\n" +
                "    'properties.bootstrap.servers' = 'node01:9092',\n" +
                "    'format' = 'json'\n" +
                ")");

        tenv.executeSql("select page_id,cnt,rn,window_start,window_end\n" +
                "from(\n" +
                "\tselect *,row_number() over(partition by window_start,window_end order by cnt desc) rn \n" +
                "\tfrom(\n" +
                "\t\tselect page_id, sum(clicks) as cnt, window_start, window_end\n" +
                "\t\t from  table (\n" +
                "\t\t tumble( table table1, descriptor(ts), INTERVAL '1' HOUR )\n" +
                "\t\t)\n" +
                "\t\tgroup by page_id, window_start, window_end \n" +
                "\t)\n" +
                ") where rn <= 2").print();
    }

}