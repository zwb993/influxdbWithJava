import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Point.Builder;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

/**
 * InfluxDB数据库连接操作类
 *
 * @author 大脑补丁
 */
public class InfluxDBConnection {

    // 用户名
    private String username;
    // 密码
    private String password;
    // 连接地址
    private String openurl;
    // 数据库
    private String database;
    // 保留策略
    private String retentionPolicy;

    private InfluxDB influxDB;

    public InfluxDBConnection(String username, String password, String openurl, String database,
                              String retentionPolicy) {
        this.username = username;
        this.password = password;
        this.openurl = openurl;
        this.database = database;
        this.retentionPolicy = retentionPolicy == null || retentionPolicy.equals("") ? "autogen" : retentionPolicy;
        influxDbBuild();
    }

    /**
     * 创建数据库
     *
     * @param dbName
     */
    @SuppressWarnings("deprecation")
    public void createDB(String dbName) {
        influxDB.createDatabase(dbName);
    }

    /**
     * 删除数据库
     *
     * @param dbName
     */
    @SuppressWarnings("deprecation")
    public void deleteDB(String dbName) {
        influxDB.deleteDatabase(dbName);
    }

    /**
     * 测试连接是否正常
     *
     * @return true 正常
     */
    public boolean ping() {
        boolean isConnected = false;
        Pong pong;
        try {
            pong = influxDB.ping();
            if (pong != null) {
                isConnected = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return isConnected;
    }

    /**
     * 连接时序数据库 ，若不存在则创建
     *
     * @return
     */
    public InfluxDB influxDbBuild() {
        if (influxDB == null) {
            influxDB = InfluxDBFactory.connect(openurl);
        }
        try {
            // if (!influxDB.databaseExists(database)) {
            // influxDB.createDatabase(database);
            // }
        } catch (Exception e) {
            // 该数据库可能设置动态代理，不支持创建数据库
            // e.printStackTrace();
        } finally {
            influxDB.setRetentionPolicy(retentionPolicy);
        }
        influxDB.setLogLevel(InfluxDB.LogLevel.NONE);
        return influxDB;
    }

    /**
     * 创建自定义保留策略
     *
     * @param policyName
     *            策略名
     * @param duration
     *            保存天数
     * @param replication
     *            保存副本数量
     * @param isDefault
     *            是否设为默认保留策略
     */
    public void createRetentionPolicy(String policyName, String duration, int replication, Boolean isDefault) {
        String sql = String.format("CREATE RETENTION POLICY \"%s\" ON \"%s\" DURATION %s REPLICATION %s ", policyName,
                database, duration, replication);
        if (isDefault) {
            sql = sql + " DEFAULT";
        }
        this.query(sql);
    }

    /**
     * 创建默认的保留策略
     *
     * 策略名：default，保存天数：30天，保存副本数量：1
     *            设为默认保留策略
     */
    public void createDefaultRetentionPolicy() {
        String command = String.format("CREATE RETENTION POLICY \"%s\" ON \"%s\" DURATION %s REPLICATION %s DEFAULT",
                "default", database, "30d", 1);
        this.query(command);
    }

    /**
     * 查询
     *
     * @param command
     *            查询语句
     * @return
     */
    public QueryResult query(String command) {
        return influxDB.query(new Query(command, database));
    }

    /**
     * 插入
     *
     * @param measurement
     *            表
     * @param tags
     *            标签
     * @param fields
     *            字段
     */
    public void insert(String measurement, Map<String, String> tags, Map<String, Object> fields, long time,
                       TimeUnit timeUnit) {
        Builder builder = Point.measurement(measurement);
        builder.tag(tags);
        builder.fields(fields);
        if (0 != time) {
            builder.time(time, timeUnit);
        }
        influxDB.write(database, retentionPolicy, builder.build());
    }

    /**
     * 批量写入测点
     *
     * @param batchPoints
     */
    public void batchInsert(BatchPoints batchPoints) {
        influxDB.write(batchPoints);
        // influxDB.enableGzip();
        // influxDB.enableBatch(2000,100,TimeUnit.MILLISECONDS);
        // influxDB.disableGzip();
        // influxDB.disableBatch();
    }

    /**
     * 批量写入数据
     *
     * @param database
     *            数据库
     * @param retentionPolicy
     *            保存策略
     * @param consistency
     *            一致性
     * @param records
     *            要保存的数据（调用BatchPoints.lineProtocol()可得到一条record）
     */
    public void batchInsert(final String database, final String retentionPolicy, final ConsistencyLevel consistency,
                            final List<String> records) {
        influxDB.write(database, retentionPolicy, consistency, records);
    }

    /**
     * 删除
     *
     * @param command
     *            删除语句
     * @return 返回错误信息
     */
    public String deleteMeasurementData(String command) {
        QueryResult result = influxDB.query(new Query(command, database));
        return result.getError();
    }

    /**
     * 关闭数据库
     */
    public void close() {
        influxDB.close();
    }

    /**
     * 构建Point
     *
     * @param measurement
     * @param time
     * @param fields
     * @return
     */
    public Point pointBuilder(String measurement, long time, Map<String, String> tags, Map<String, Object> fields) {
        Point point = Point.measurement(measurement).time(time, TimeUnit.MILLISECONDS).tag(tags).fields(fields).build();
        return point;
    }


    public static void main(String[] args) {
//        influxSelect();
        influxInsert();
    }

    private static void influxSelect() {
        InfluxDBConnection influxDBConnection = new InfluxDBConnection("", "", "http://127.0.0.1:8086", "huodian", "");
        System.out.println(System.currentTimeMillis());
        QueryResult results = influxDBConnection
                .query("SELECT \"value\" FROM \"sensor_history\" " +
                        "where \"kks\"='4DCS.40CFB41GH2' and \"time\" < 1521303600000000000 and \"time\" > 1514003600000000000");
        //results.getResults()是同时查询多条SQL语句的返回值，此处我们只有一条SQL，所以只取第一个结果集即可。
        QueryResult.Result oneResult = results.getResults().get(0);
        System.out.println(System.currentTimeMillis());
        if (oneResult.getSeries() != null) {
            List<List<Object>> valueList = oneResult.getSeries().stream().map(QueryResult.Series::getValues)
                    .collect(Collectors.toList()).get(0);
            if (valueList != null && valueList.size() > 0) {
                for (List<Object> value : valueList) {
                    Map<String, String> map = new HashMap<String, String>();
                    // 数据库中字段1取值
                    String field1 = value.get(0) == null ? null : value.get(0).toString();
                    // 数据库中字段2取值
                    String field2 = value.get(1) == null ? null : value.get(1).toString();
                    // TODO 用取出的字段做你自己的业务逻辑……
                }
            }
        }
    }

    private static void influxInsert() {
        InfluxDBConnection influxDBConnection = new InfluxDBConnection("admin", "admin",
                "http://127.0.0.1:8096", "huodian", "");
        for(int i=0; i<1200; i++) {
            List<String> records = new ArrayList<String>();
            BatchPoints batchPoints1 = BatchPoints.database("huodian")
                    .retentionPolicy("two_year").consistency(InfluxDB.ConsistencyLevel.ALL).build();
            for (int j = 0; j < 100; j++) {

                Map<String, String> tags1 = new HashMap<String, String>();
                for (int z = 0; z < 1000; z++) {

                    String kks = "4DCS.40CFB41GHas" + z;
                    tags1.put("kks", kks);
                    Map<String, Object> fields1 = new HashMap<>(2);
                    fields1.put("value", z + 0.0);
                    fields1.put("status", 0);
                    // 一条记录值
                    Point point1 = influxDBConnection.pointBuilder("sensor_history", 1580092115000L + j * 1000 + i*100000, tags1, fields1);
                    batchPoints1.point(point1);
                }

            }
            records.add(batchPoints1.lineProtocol());
            influxDBConnection.batchInsert("huodian", "", InfluxDB.ConsistencyLevel.ALL, records);
        }
    }

}


