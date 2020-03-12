import org.influxdb.dto.QueryResult;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author zhengweibing3
 * @version V1.0.0
 * @description:
 * @date 2020/3/12 10:33
 * @copyright Copyright © 2019  智能城市icity.jd.com ALL Right Reserved
 */
public class MysqlAndInfluxdbCompare {

    public static void main(String[] args) {

        for(int i=0;i<100; i++) {
            int index= 10000;
//            selectResultList(index*i);
            influxSelect(index*i);
        }
    }

    public static void selectResultList(long second){
        Date start = new Date();
        System.out.println("开始时间：" + start);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Long startLong = 1515513960000L;
        String startTime = dateFormat.format(new Date(startLong));
        String entTime = dateFormat.format(new Date(startLong+second));
        String strValue = "value->'$.\"4DCS.40HCB11AT002-LP\".value' as `value`,";
        String select = "select `time`,";
        String sql = select + strValue + " from history_sensor_value where time between "+startTime +" and "+ entTime;
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = MysqlConnection.getConnection();
            pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally{
            MysqlConnection.close(pstmt);
            MysqlConnection.close(conn);
        }
        System.out.printf("mysql查询%d条数据，使用了：%d毫秒%n", second, System.currentTimeMillis()-start.getTime());
    }

    private static void influxSelect(long second) {
        InfluxDBConnection influxDBConnection = new InfluxDBConnection("", "", "http://127.0.0.1:8086", "huodian", "");
        Date start = new Date();
        Long endLong = 1520513598000000000L;
        long startLong = endLong-second*1000000000;
        QueryResult results = influxDBConnection
                .query("SELECT \"value\" FROM \"sensor_history\" " +
                        "where \"kks\"='4DCS.40CFB41GH2' and \"time\" < "+endLong+" and \"time\" > "+startLong);
        QueryResult.Result oneResult = results.getResults().get(0);
        System.out.printf("mysql查询%d条数据，使用了：%d毫秒%n", second, System.currentTimeMillis()-start.getTime());
    }

}
