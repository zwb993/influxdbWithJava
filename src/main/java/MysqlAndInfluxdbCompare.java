import org.influxdb.dto.QueryResult;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author zhengweibing3
 * @version V1.0.0
 * @description:
 * @date 2020/3/12 10:33
 * @copyright Copyright © 2019  智能城市icity.jd.com ALL Right Reserved
 */
public class MysqlAndInfluxdbCompare {

    public static void main(String[] args) {

        for(int i=1;i<100; i++) {
            int index= 10000;
//            selectResultList(index*i);
            influxSelect(index*i);
        }
    }

    public static void selectResultList(long second){
        Date start = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Long startLong = 1515513960000L;
        String startTime = dateFormat.format(new Date(startLong));
        String entTime = dateFormat.format(new Date(startLong+second*1000));
        String strValue = "value->'$.\"4DCS.40HCB11AT002-LP\".value' as `value`";
        String select = "select `time`,";
        String sql = select + strValue + " from history_sensor_value where time between \""+startTime +"\" and \""+ entTime+ "\"";
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            conn = MysqlConnection.getConnection();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
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
                        "where \"kks\"='4DCS.40CFB41GH2' and \"time\" <= "+endLong+" and \"time\" >= "+startLong);
        QueryResult.Result oneResult = results.getResults().get(0);
        List<List<Object>> valueList = new ArrayList<>();
        System.out.printf("influxdb查询%d条数据，使用了：%d毫秒%n", second, System.currentTimeMillis()-start.getTime());
    }

}
