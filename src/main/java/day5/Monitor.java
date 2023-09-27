package day5;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Monitor {

    //{"action_time":1682219447,"monitor_id":"0001","camera_id":"1","car":"豫A12345","speed":34.5,"road_id":"01","area_id":"20"}
    private String monitorId;   //规范  monitor_id  --> monitorId

    private String cameraId;

    private String car;

    private Double speed;

    private String roadId;

    private String areaId;

    private Long actionTime;


}