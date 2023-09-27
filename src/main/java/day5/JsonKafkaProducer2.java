package day5;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * @Auther: Li
 * @Date: 2023/8/30 18:45
 * @Desc:
 **/
public class JsonKafkaProducer2 {


    public static void main(String[] args) throws InterruptedException {
        go("node01:9092","topic-car",Integer.MAX_VALUE);
    }

    private static int batch = 1000;

    /**
     *     again
     *
     * @param
     * @return
     */
    public static void go(String boostrapServer , String topic , int count ) throws InterruptedException {


        // 这个里面放置配置相关信息
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,boostrapServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        List<Monitor> batchData = new ArrayList<>();

        //循环发送消息
        Random random = new Random();

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < count; i++) {
            long timeIncrement = (random.nextInt(10) + 1) *1000   ; // 随机的递增幅度 1-10秒
            long actionTime = startTime + timeIncrement;

            //为了递增
            startTime = actionTime;

            Monitor carTemp = new Monitor();
            carTemp.setActionTime(actionTime);
            carTemp.setMonitorId(String.format("%04d", random.nextInt(10))) ;
            carTemp.setCameraId(String.valueOf(random.nextInt(10) + 1)) ;
            carTemp.setCar("豫A" + String.format("%05d", random.nextInt(100000)));
            carTemp.setSpeed(random.nextDouble() * 100);
            carTemp.setRoadId(String.format("%02d", random.nextInt(50) + 1));
            carTemp.setAreaId(String.format("%02d", random.nextInt(30) + 1));
            System.out.println(JSON.toJSONString(carTemp));
            kafkaProducer.send(new ProducerRecord<>(topic , JSON.toJSONString(carTemp)));
            Thread.sleep(800);
        }
        kafkaProducer.close();
    }


}

