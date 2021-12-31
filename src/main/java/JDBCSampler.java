import org.apache.jmeter.engine.event.LoopIterationEvent;
import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.property.JMeterProperty;
import org.apache.jmeter.testelement.property.PropertyIterator;
import org.apache.jorphan.collections.Data;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class JDBCSampler extends AbstractSampler
{
    public static final String URL = "JDBCSampler.url";
    public static final String DRIVER = "JDBCSampler.driver";
    public static final String QUERY = "JDBCSampler.query";

    public static final String JDBCSAMPLER_PROPERTY_PREFIX = "JDBCSampler.";
    public static final String CONNECTION_POOL_IMPL =
            JDBCSAMPLER_PROPERTY_PREFIX + "connPoolClass";
    private static final PlaceholderResolver defaultResolver = new PlaceholderResolver();
    static Random rand = new Random();

    /**
     * Creates a JDBCSampler.
     */
    public JDBCSampler()
    {}

    public SampleResult sample(Entry e)
    {
        SampleResult res = new SampleResult();
        res.setSampleLabel(getName());
        res.setSamplerData(this.toString());

        res.sampleStart();
        Connection conn = null;
        PreparedStatement pstmt = null;
        try
        {
            // TODO: Consider creating a sub-result with the time to get the
            //       connection.
            conn = TestPlanLauncher.hikariDataSource.getConnection();
            conn.setAutoCommit(false);
            StringBuilder strBuild = new StringBuilder();
            if(TestReadConfig.Insert_Type== 1){
                for(int j=0;j<TestReadConfig.Multi_Insert_Count;++j) {
                    String str1=defaultResolver.resolveByRule(TestReadConfig.Sql_Str,PlaceholderResolver::getRandomByConfig);
                    String str2=defaultResolver.resolveByRule(TestReadConfig.Sql_Str2,PlaceholderResolver::getRandomByConfig);
                    String time=PlaceholderResolver.RandomTime();

                    for (int i = 1; i < TestReadConfig.Multi_Count; ++i) {
                        String str3 = String.format("%0" + 3 + "d", Integer.parseInt(String.valueOf(i)) + 1);
                        String str=str1+time+str3+str2;
                        strBuild.append(str);
                    }
                    System.out.println(strBuild);
                    strBuild.append("commit;");
                }

            }else if(TestReadConfig.Insert_Type== 2 ){
                conn.setAutoCommit(false);
                String str1=defaultResolver.resolveByRule(TestReadConfig.Sql_Str, PlaceholderResolver::getRandomByConfig);
                String str2=defaultResolver.resolveByRule(TestReadConfig.Sql_Str2, PlaceholderResolver::getRandomByConfig);
                String[] split = str1.split(",");
                String str=str1+split[1]+str2;
                pstmt=conn.prepareStatement(str);

                for(int i=1;i<=TestReadConfig.Multi_Insert_Count;i+=1){
                    pstmt.clearBatch();
                    String time=PlaceholderResolver.RandomTime();
                    for (int j=0;j<TestReadConfig.Multi_Count;j++){
                        String str3 = String.format("%0" + 3 + "d", Integer.parseInt(String.valueOf(j)) + 1);
                        String time1=time+str3;
                        pstmt.setString(1, time1);
                        pstmt.addBatch();
                    }
                    pstmt.executeBatch();
                  //  if ((i % TestReadConfig.Multi_Insert_Count== 0)){
                        conn.commit();
                   // }
                }
            }else if(TestReadConfig.Insert_Type== 3){
                conn.setAutoCommit(false);
                pstmt = conn.prepareStatement(TestReadConfig.AddInFile);
                com.mysql.cj.jdbc.JdbcStatement testStmt = pstmt.unwrap(com.mysql.cj.jdbc.StatementImpl.class);
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <=TestReadConfig.Multi_Insert_Count ; i++) {
                    String str=defaultResolver.resolveByRule(TestReadConfig.Str1, PlaceholderResolver::getRandomByConfig);
                    String[] split = str.split(",");
                    String time=PlaceholderResolver.RandomTime();
                    for (int j=0;j<TestReadConfig.Multi_Count;j++){
                        sb.append(str).append(time).append(String.format("%0" + 3 + "d", Integer.parseInt(String.valueOf(j)) + 1)).append(",E,T,").append(split[1]).append(defaultResolver.resolveByRule(TestReadConfig.Str2, PlaceholderResolver::getRandomByConfig)).append("\n");
                    }
                    if ((i % TestReadConfig.Multi_Insert_Count== 0)){
                        InputStream is = new ByteArrayInputStream(sb.toString().getBytes());
                        testStmt.setLocalInfileInputStream(is);
                        pstmt.execute();
                        conn.commit();
                        sb.setLength(0);
                    }
                }
            }else if(TestReadConfig.Insert_Type== 4){
                conn.setAutoCommit(false);
                pstmt = conn.prepareStatement(TestReadConfig.AddInFile);
                com.mysql.cj.jdbc.JdbcStatement testStmt = pstmt.unwrap(com.mysql.cj.jdbc.StatementImpl.class);
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <=TestReadConfig.Multi_Insert_Count ; i++) {
                    String str=defaultResolver.resolveByRule(TestReadConfig.Str3, PlaceholderResolver::getRandomByConfig);
                    String[] split = str.split(",");
                    String time=PlaceholderResolver.RandomTime();
                    for (int j=0;j<TestReadConfig.Multi_Count;j++){
                        String str4=TestReadConfig.map.get("1");
                        List<Integer> listIds = Arrays.asList(str4.split(",")).stream().map(s -> Integer.parseInt(s.trim())).collect(Collectors.toList());
                        int randnum1=rand.nextInt(listIds.get(1)-listIds.get(0)) + listIds.get(0);
                        String randnum = String.format("%0" + listIds.get(2)+ "d",randnum1 );
                        sb.append("E").append(randnum).append(str).append(time).append(String.format("%0" + 3 + "d", Integer.parseInt(String.valueOf(j)) + 1)).append(",E,T,").append(split[1]).append(defaultResolver.resolveByRule(TestReadConfig.Str2,placeholderValue->String.valueOf(PlaceholderResolver.getRandomByConfig(placeholderValue)))).append("\n");
                    }
                    if ((i % TestReadConfig.Multi_Insert_Count== 0)){
                        InputStream is = new ByteArrayInputStream(sb.toString().getBytes());
                        testStmt.setLocalInfileInputStream(is);
                        pstmt.execute();
                        conn.commit();
                        sb.setLength(0);
                    }
                }
            }else if(TestReadConfig.Insert_Type== 5 ){
                conn.setAutoCommit(false);
                String str1=defaultResolver.resolveByRule(TestReadConfig.Sql_Str3, PlaceholderResolver::getRandomByConfig);
                String str2=defaultResolver.resolveByRule(TestReadConfig.Sql_Str2, PlaceholderResolver::getRandomByConfig);
                String[] split = str1.split(",");
                String str=str1+split[1]+str2;
                pstmt=conn.prepareStatement(str);

                for(int i=1;i<=TestReadConfig.Multi_Insert_Count;i+=1){
                    pstmt.clearBatch();
                    String time=PlaceholderResolver.RandomTime();
                    for (int j=0;j<TestReadConfig.Multi_Count;j++){
                        String str3 = String.format("%0" + 3 + "d", Integer.parseInt(String.valueOf(j)) + 1);
                        String time1=time+str3;
                        String str4=TestReadConfig.map.get("1");
                        List<Integer> listIds = Arrays.asList(str4.split(",")).stream().map(s -> Integer.parseInt(s.trim())).collect(Collectors.toList());
                        int randnum1=rand.nextInt(listIds.get(1)-listIds.get(0)) + listIds.get(0);
                        String randnum = String.format("%0" + listIds.get(2)+ "d",randnum1 );
                        String qcd="E"+randnum+"#0/T";
                        pstmt.setString(1,qcd);
                        pstmt.setString(2, time1);
                        pstmt.addBatch();
                    }
                    pstmt.executeBatch();
                  //  if ((i % TestReadConfig.Multi_Insert_Count== 0)){
                    conn.commit();
                //    }
                }
            }else if(TestReadConfig.Insert_Type== 6 ){
                conn.setAutoCommit(false);
                pstmt=conn.prepareStatement(TestReadConfig.Batch_pstmt);
                String pstmtStr1=defaultResolver.resolveByRule(TestReadConfig.Str1, PlaceholderResolver::getRandomByConfig);
                String pstmtStr2=defaultResolver.resolveByRule(TestReadConfig.Str2, PlaceholderResolver::getRandomByConfig);

                for(int i=1;i<=TestReadConfig.Multi_Insert_Count;i+=1){
                    pstmt.clearBatch();
                    String time=PlaceholderResolver.RandomTime();
                    for (int j=0;j<TestReadConfig.Multi_Count;j++){
                        String str3 = String.format("%0" + 3 + "d", Integer.parseInt(String.valueOf(j)) + 1);
                        String time1=time+str3;
                        String pstmtStr=pstmtStr1+time1+",E,T,"+pstmtStr1.split(",")[1]+pstmtStr2+";";
                        String[] split=pstmtStr.split(",");
                        for (int k=0;k< split.length-1;k++){
                            pstmt.setString(k+1,split[k]);}
                        pstmt.addBatch();
                    }
                    pstmt.executeBatch();
              //      if ((i % TestReadConfig.Multi_Insert_Count== 0)){
                    conn.commit();
                //    }
                }
            }else if(TestReadConfig.Insert_Type== 7 ){
                conn.setAutoCommit(false);
                String str1=defaultResolver.resolveByRule(TestReadConfig.Str3, PlaceholderResolver::getRandomByConfig);
                String str2=defaultResolver.resolveByRule(TestReadConfig.Str2, PlaceholderResolver::getRandomByConfig);
                pstmt=conn.prepareStatement(TestReadConfig.Batch_pstmt);

                for(int i=1;i<=TestReadConfig.Multi_Insert_Count;i+=1){
                    pstmt.clearBatch();
                    String time=PlaceholderResolver.RandomTime();
                    for (int j=0;j<TestReadConfig.Multi_Count;j++){
                        String str3 = String.format("%0" + 3 + "d", Integer.parseInt(String.valueOf(j)) + 1);
                        String time1=time+str3;
                        String str4=TestReadConfig.map.get("1");
                        List<Integer> listIds = Arrays.asList(str4.split(",")).stream().map(s -> Integer.parseInt(s.trim())).collect(Collectors.toList());
                        int randnum1=rand.nextInt(listIds.get(1)-listIds.get(0)) + listIds.get(0);
                        String randnum = String.format("%0" + listIds.get(2)+ "d",randnum1 );
                        String pstmtStr='E'+randnum+str1+time1+",E,T,"+str1.split(",")[1]+str2+';';
                        String[] split=pstmtStr.split(",");
                        for (int k=0;k< split.length-1;k++){
                            pstmt.setString(k+1,split[k]);}
                        pstmt.addBatch();
                    }
                    pstmt.executeBatch();
                    //if ((i % TestReadConfig.Multi_Insert_Count== 0)){
                        conn.commit();
                    //}
                }
            }
            int[] types = {2,3,4,5,6,7};
            if(Arrays.binarySearch(types,TestReadConfig.Insert_Type)<0) {
            pstmt = conn.prepareCall(strBuild.toString());

             //System.out.println("thread start"+ this.getThreadName());

            // Based on query return value, get results
            if (pstmt.execute())
            {
                ResultSet rs = null;
                try
                {
                    rs = pstmt.getResultSet();
                    Data data = getDataFromResultSet(rs);
                    res.setResponseData(data.toString().getBytes());
                }
                finally
                {
                    if (rs != null)
                    {
                        try
                        {
                            rs.close();
                        }
                        catch (SQLException exc)
                        {
//                            log.warn("Error closing ResultSet", exc);
                        }
                    }
                }
            }
            else
            {
                int updateCount = pstmt.getUpdateCount();
                String results = updateCount + " updates";
                res.setResponseData(results.getBytes());
            }
            }

            res.setDataType(SampleResult.TEXT);
            res.setSuccessful(true);
        }
        catch (Exception ex)
        {
//            log.error("Error in JDBC sampling", ex);
            ex.printStackTrace();
            res.setResponseData(new byte[0]);
            res.setSuccessful(false);
        }
        finally
        {
            if (pstmt != null)
            {
                try
                {
                    pstmt.close();
                }
                catch (SQLException err)
                {
                    pstmt = null;
                }
            }

            if (conn != null)
            {
                try {
                    conn.close();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }

            }
        }

        res.sampleEnd();
        return res;
    }

    private Map getJDBCProperties()
    {
        Map props = new HashMap();

        PropertyIterator iter = propertyIterator();
        while (iter.hasNext())
        {
            JMeterProperty prop = iter.next();
            if (prop.getName().startsWith(JDBCSAMPLER_PROPERTY_PREFIX))
            {
                props.put(prop.getName(), prop);
            }
        }
        return props;
    }

    /**
     * Gets a Data object from a ResultSet.
     *
     * @param  rs ResultSet passed in from a database query
     * @return    a Data object
     * @throws    java.sql.SQLException
     */
    private Data getDataFromResultSet(ResultSet rs) throws SQLException
    {
        ResultSetMetaData meta = rs.getMetaData();
        Data data = new Data();

        int numColumns = meta.getColumnCount();
        String[] dbCols = new String[numColumns];
        for (int i = 0; i < numColumns; i++)
        {
            dbCols[i] = meta.getColumnName(i + 1);
            data.addHeader(dbCols[i]);
        }

        while (rs.next())
        {
            data.next();
            for (int i = 0; i < numColumns; i++)
            {
                Object o = rs.getObject(i + 1);
                if (o instanceof byte[])
                {
                    o = new String((byte[]) o);
                }
                data.addColumnValue(dbCols[i], o);
            }
        }
        return data;
    }


    public String getDriver()
    {
        return "com.mysql.jdbc.Driver";
    }

    public String getUrl()
    {
        return "127.0.0.1";
    }

    public String getUsername()
    {
        return "root";
    }

    public String getPassword()
    {
        return "123456";
    }

    public String getQuery()
    {
        return this.getPropertyAsString(QUERY);
    }


    public String toString()
    {
        return getUrl() + ", user: " + getUsername() + "\n" + getQuery();
    }


    public void testStarted(String host)
    {
        testStarted();
    }

    public synchronized void testStarted()
    {
        /*
         * Test started is called before the thread data has been set up, so cannot
         * rely on its values being available.
         */
//     log.debug("testStarted(), thread: "+Thread.currentThread().getName());
//        // The first call to getKey for a given key will set up the connection
//        // pool.  This can take awhile, so do it while the test is starting
//        // instead of waiting for the first sample.
//        try
//        {
//            getKey();
//        }
//        catch (ConnectionPoolException e)
//        {
//            log.error("Error initializing database connection", e);
//        }
    }

    public void testEnded(String host)
    {
        testEnded();
    }

    public synchronized void testEnded()
    {
//        log.debug("testEndded(), thread: "+Thread.currentThread().getName());
    }

    public void testIterationStart(LoopIterationEvent event)
    {
    }
}