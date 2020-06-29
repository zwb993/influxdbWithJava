import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author zhengweibing3
 * @version V1.0.0
 * @description:
 * @date 2020/6/28/0028 14:29
 * @copyright Copyright © 2019  智能城市icity.jd.com ALL Right Reserved
 */
public class PostgresqlConnection {

    private final static String KKS_BASE = "4dcs.asdfghasdg-";
    private final static int MS = 1000;

    public static void main(String[] args) {
        try {
            Long START_LONG = 1561707084000L;
            int BATCH_COUNT = 1000;
            int READ_COUNT = 4000;
            int KKS_NUM = 1000;
            String TABLE_NAME = "table_9";
            createTable(TABLE_NAME);
            for (int i = 0; ; i++) {

                // print status
                System.out.println(String.format(
                    "iter: %d, count: %d",
                    i,
                    i * BATCH_COUNT
                ));

                // insert data
                Date dateStartWrite = new Date();
                Date date = new Date(START_LONG + i * BATCH_COUNT * MS);
                insertData(TABLE_NAME, KKS_NUM, BATCH_COUNT, date);
                Date dateEndWrite = new Date();

                // calc time
                System.out.println("Write: " + String.valueOf(dateEndWrite.getTime() - dateStartWrite.getTime()));

                // read data
                Date timerStartRead = new Date();

                Long readTimestampLong = START_LONG + (i / 2) * BATCH_COUNT * 1000;
                Date dateStartRead = new Date(readTimestampLong);
                Date dateEndRead = new Date(readTimestampLong + READ_COUNT * 1000);

                selectData(
                    TABLE_NAME,
                    dateStartRead,
                    dateEndRead,
                    (int)(Math.random()*KKS_NUM)
                );

                Date timerEndRead= new Date();
                System.out.println("Read: " + String.valueOf(timerEndRead.getTime() - timerStartRead.getTime()));

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
//        insertDataPow(200000L);
    }

    private static Connection getPostgresqlConnect() {
        Connection c = null;
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/postgres",
                            "postgres", "123456");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return c;
    }

    private static void insertData(String tableName, int kksNum, int batchCount, Date date) throws SQLException {
        Connection c;
        c = getPostgresqlConnect();

        String INSERT_SQL = String.format(
            "INSERT INTO %s(time, kks,value) VALUES (?, ?, ?)",
            tableName
        );
        PreparedStatement ps = c.prepareStatement(INSERT_SQL);

        for (int j = 0; j < batchCount; j++) {
            for (int i=0; i < kksNum; i++) {
                ps.setTimestamp(1, new Timestamp(date.getTime() + j * MS));
                ps.setString(2,  KKS_BASE + i);
                ps.setDouble(3, i);
                ps.addBatch();
            }
        }
        ps.clearParameters();
        ps.executeBatch();
        c.close();
    }
    private static void insertDataPow(Long timeRange){
        Connection conn = null;
        PreparedStatement pstmt = null;
        Double index = 1d;
        try {
            for (int i=1; index < timeRange; i++) {
                System.out.print("开始时间："+new Date() + "    ");
                String sqlCopy = "INSERT INTO sensor_data ( time, kks, value) SELECT time + " + index.intValue() + " * interval  '1 second' as time, kks, value FROM sensor_data;";
//                String sqlCopy = "INSERT INTO sensor_data ( time, kks, value) SELECT time + " + index.intValue() + " * interval  '1 second' as time, kks, value FROM sensor_data;";
                conn = getPostgresqlConnect();
                pstmt = conn.prepareStatement(sqlCopy);
                pstmt.execute();
                System.out.print("复制了"+index+"的数据了");
                System.out.println(",结束时间:"+new Date());
                index = Math.pow(2, i);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    private static void selectData(String tableName, Date startDate, Date endDate, int kksId) {
        try {
            Connection c;

            c = getPostgresqlConnect();
            Statement stmt = c.createStatement();

            DateFormat dFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String startDateString = dFormat.format(startDate);
            String endDateString = dFormat.format(endDate);
            String sql = String.format("select * from %s" +
                " where " +
                "kks = '%s'" +
                " and " +
                "time between to_timestamp('%s','YYYY-MM-DD hh24:mi:ss')" +
                " and " +
                "to_timestamp('%s','YYYY-MM-DD hh24:mi:ss')",
                tableName,
                KKS_BASE+kksId,
                startDateString,
                endDateString
            );
            stmt.executeQuery(sql);
            c.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private static void createTable(String tableName) throws SQLException {
        Connection connection = getPostgresqlConnect();
        Statement stmt = connection.createStatement();
        String createTableSql = String.format(
            "CREATE TABLE %s (\n" +
            " time TIMESTAMPTZ NOT NULL,\n" +
            " kks TEXT NOT NULL,\n" +
            " value DOUBLE PRECISION  NULL,\n" +
            " primary key (kks, time)\n" +
            ");",
            tableName
        );

        String createHyperTable = String.format(
            "SELECT create_hypertable('%s', 'time', 'kks', 2,  chunk_time_interval => 10000000000);",
            tableName
        );
        stmt.execute(createTableSql);
        stmt.execute(createHyperTable);
    }
}
