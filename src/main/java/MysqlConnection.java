import java.sql.*;
import java.util.*;
import java.util.Date;

/**
 * @author zhengweibing3
 * @version V1.0.0
 * @description:
 * @date 2019/11/25 18:50
 * @copyright Copyright © 2019  智能城市icity.jd.com ALL Right Reserved
 */
public class MysqlConnection {

    public static void main(String[] args) throws SQLException {
//        insertActionBatch(1000L, getActionString());

//        selectResultList();
        addData(1500000);

    }

    /**
     * 添加记录
     * @param timeRange 要插入的时间范围
     */
    public static void addData(long timeRange) throws SQLException {

        StringBuilder str = new StringBuilder();

        str.append("{\"DCSHCB11AT001\":{\"value\":32.3197,\"status\":0},");

        for (int i = 2; i < 300; i++) {
            if(i<10) {
                str.append("\"4DCS.40HCB11AT00" + i + "-LP\":{\"value\":" + (32.42 + i) + ",\"status\":" + i % 2 + "},");
            }else{
                str.append("\"4DCS.40HCB11AT0" + i + "-LP\":{\"value\":" + (32.42 + i) + ",\"status\":" + i % 2 + "},");
            }
        }
        str.append("\"4DCS.40HCB11AT130-LP\":{\"value\":600.1324,\"status\":0}}");

        insertDataPow(timeRange);
//        insertBatch(timeRange, str.toString());
    }

    /**
     * 存储过程
     * @param timeRange
     */
    private static void insertDataPow(Long timeRange){
        Connection conn = null;
        PreparedStatement pstmt = null;
        double index = 1;
        try {
            for (int i=1; index < timeRange; i++) {
                System.out.print("开始时间："+new Date() + "    ");
                String sqlCopy = "INSERT INTO history_sensor_value ( time, `value`) SELECT DATE_ADD(time, INTERVAL " + index * 1 + " SECOND ) as `time`,`value` FROM history_sensor_value;";
                conn = MysqlConnection.getConnection();
                pstmt = conn.prepareStatement(sqlCopy);
                pstmt.execute();
                System.out.print("复制了"+index+"的数据了");
                System.out.println(",结束时间:"+new Date());
                index = Math.pow(2, i);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        finally{
            close(pstmt);
            close(conn);
        }
    }

    private static void insertActionBatch(Long timeRange, String str) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = "insert into history_action_value(time,value,unit_no) values(?,?, 4)";
        try{
            conn = MysqlConnection.getConnection();
            pstmt = conn.prepareStatement(sql);
            for(int i=0;i<timeRange;i++){
                pstmt.setTimestamp(1,new Timestamp((1514736000L+i*20)*1000));
                pstmt.setString(2, str);
                pstmt.addBatch();
            }
            pstmt.executeBatch();

        }catch(SQLException e){
            e.printStackTrace();
        }
        finally{
            MysqlConnection.close(pstmt);
            MysqlConnection.close(conn);
        }
    }

    /**
     * 批量插入
     * @param timeRange
     * @param str
     */
    private static void insertBatch(Long timeRange, String str){
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = "insert into history_sensor_value(time,value,unit_no) values(?,?, 4)";
        try{
            conn = MysqlConnection.getConnection();
            pstmt = conn.prepareStatement(sql);
            for(int i=0;i<timeRange;i++){
                pstmt.setTimestamp(1,new Timestamp((1483260472L+i*20)*1000));
                pstmt.setString(2, str);
                pstmt.addBatch();
//                if(i%100000==0){
//                    pstmt.executeBatch();
//                    System.out.println("现在已经加了"+i+ "数据");
//                }
            }
            pstmt.executeBatch();

        }catch(SQLException e){
            e.printStackTrace();
        }
        finally{
            MysqlConnection.close(pstmt);
            MysqlConnection.close(conn);
        }
    }

    /**
     * 查询课程
     * @return
     */
    public static List<HistoryValue> selectResultList(){
        Date start = new Date();
        System.out.println("开始时间：" + start);
        String strValue = "value->'$.\"4DCS.40HCB11AT002-LP\".value' as `value`,";
        String strValueWithSp = "value->'$.DCSHCB11AT001.value' as `value`,";
        String select = "select `time`,";
        String sql = select + strValue + " from history_sensor_value where time between '2018-01-01 00:00:00' and '2018-01-02 00:00:00'";
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs ;
        //创建一个集合对象用来存放查询到的数据
        List<HistoryValue> historyValueList = new ArrayList<>();
        try {
            conn = MysqlConnection.getConnection();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()){
                Timestamp date =rs.getTimestamp("time");
//                Double value = rs.getDouble("value");
                int unitNo = rs.getInt("unit_no");
                //每个记录对应一个对象
                HistoryValue historyValue = new HistoryValue();
//                historyValue.setValue(value);
                historyValue.setTime(date);
                historyValue.setUnitNo(unitNo);
                //将对象放到集合中
                historyValueList.add(historyValue);
            }
        } catch (SQLException e) {
            // TODO: handle exception
            e.printStackTrace();
        }finally{
            MysqlConnection.close(pstmt);
            MysqlConnection.close(conn);
        }
        System.out.println("查询使用了：" + (System.currentTimeMillis()-start.getTime()) +"毫秒");
        return historyValueList;
    }

    /**
     * 取得数据库的连接
     * @return 一个数据库的连接
     */
    public static Connection getConnection(){
        Connection conn = null;
        try {
            //初始化驱动类com.mysql.jdbc.Driver
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/huodian?characterEncoding=UTF-8&useSSL=true","root", "root");
            //该类就在 mysql-connector-java-5.0.8-bin.jar中,如果忘记了第一个步骤的导包，就会抛出ClassNotFoundException
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }
    /**
     * 封装三个关闭方法
     * @param pstmt
     */
    public static void close(PreparedStatement pstmt){
        if(pstmt != null){
            try{
                pstmt.close();
            }catch(SQLException e){
                e.printStackTrace();
            }

        }
    }

    public static void close(Connection conn){
        if(conn != null){
            try {
                conn.close();
            } catch (SQLException e) {
                // TODO: handle exception
                e.printStackTrace();
            }
        }
    }

    public static void close(ResultSet rs){
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                // TODO: handle exception
                e.printStackTrace();
            }
        }
    }

    private static String getActionString() {
        String str = "{\"4DCS.40HFE26AA102AI\":15.941984825685317,\"4DCS.40HFE23AA102AI\":22.239370663147863,\"4DCS.40HFE25AA102AI\":35.44522012104972," +
                "\"4DCS.40HFE21AA102AI\":0.0,\"4DCS.40HFE22AA102AI\":23.02332530559642,\"4DCS.40HFE24AA304AI\":41.73699014397779," +
                "\"B_layer_damper\":26.966012700150245,\"induced_draft_fan\":73.20904424168772,\"firepot_soot\":57.08659960806763," +
                "\"4DCS.40HFB16AF001AI\":50.42838545253849,\"total_soot\":40.74774824532208,\"4DCS.40HBK73AA101AVG\":90.45773658577998," +
                "\"4DCS.40HFB12AF001AI\":44.084707436488436,\"4DCS.40HFB14AF001AI\":51.25175340633212,\"4DCS.40HFC12AJ002AI\":48.82627583159944," +
                "\"preheater_soot\":38.87181295386123,\"4DCS.40HFC14AJ002AI\":51.63620772726234,\"4DCS.40HFC16AJ002AI\":52.57584395528302," +
                "\"4DCS.40HFE26AA101AI\":72.47650642736195,\"4DCS.40LAE12AA501AI\":25.339799241118833,\"4DCS.40LAE13AA501AI\":1.5022026278600453E-4," +
                "\"blower_fan\":36.28712387307085,\"4DCS.40HFE23AA101AI\":43.5083830838892,\"4DCS.40HFE21AA101AI\":0.0," +
                "\"4DCS.40LAE14AA501AI\":19.498135668455344,\"4DCS.40HFE25AA101AI\":65.41849059034787,\"4DCS.40LAE11AA501AI\":0.0075220371691804774," +
                "\"4DCS.40HFE22AA101AI\":45.197674009793154,\"predict_effi\":91.89737013254573,\"4DCS.41HLB12AA101AI\":62.274260358087254," +
                "\"4DCS.41HLB11AA101AI\":62.274260358087254,\"A_layer_damper\":10.417799254847822,\"transfer_soot\":26.284832174037387," +
                "\"4DCS.40HBK72AA101AVG\":99.41999912261963,\"4DCS.40HFB15AF001AI\":50.190480817935025,\"C_layer_damper\":39.247415600584354," +
                "\"coal_mill_comb\":[[0,1,1,1,1,1]],\"D_layer_damper\":43.28167791861949,\"predict_soot\":9.902847101604905," +
                "\"4DCS.40HFE24AA302AI\":37.58522675475649,\"F_layer_damper\":31.89271231810085,\"E_layer_damper\":22.733365353975948," +
                "\"4DCS.40HFB11AF001AI\":1.287481961521926E-132,\"predict_Nox\":417.6237588489478,\"4DCS.40HFB13AF001AI\":45.10827338533052," +
                "\"burnout_wind_damper1\":62.09856572332685,\"4DCS.40HFC11AJ002AI\":0.0,\"burnout_wind_damper2\":59.96802068166937," +
                "\"4DCS.40HFC13AJ002AI\":48.79787060180437,\"4DCS.40HFC15AJ002AI\":52.78511308107934,\"4DCS.40HCB11AT0299-LP\":123.123, " +
                "\"4DCS.40HCB11AT002-LP\":321.321}";
        return str;
    }
}
