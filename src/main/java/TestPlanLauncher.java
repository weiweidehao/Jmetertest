import com.zaxxer.hikari.HikariDataSource;
import org.apache.jmeter.JMeter;
import org.apache.jmeter.control.LoopController;
import org.apache.jmeter.engine.StandardJMeterEngine;
import org.apache.jmeter.reporters.ResultCollector;
import org.apache.jmeter.reporters.Summariser;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jmeter.testelement.TestPlan;
import org.apache.jmeter.threads.ThreadGroup;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.collections.HashTree;

import java.io.File;

public class TestPlanLauncher {
    static HikariDataSource hikariDataSource=new HikariDataSource();

    public static void main(String[] args) {
        try {
            //TestReadConfig.readConfFileByLines("./test.cnf");
            TestReadConfig.readConfFileByLines("./test.cnf");

            // c3p0 Connection pool
            CreateThreadPool();
            System.in.read();
            // jemter engine
            StandardJMeterEngine standardJMeterEngine = new StandardJMeterEngine();
            // JMETER_NON_GUI
            System.setProperty(JMeter.JMETER_NON_GUI, "true");

            //File jmeterPropertiesFile = new File("./jmeter.properties");

            String path = TestPlanLauncher.class.getClassLoader().getResource("jmeter.properties").getPath();
            File jmeterPropertiesFile = new File(path);

            if (jmeterPropertiesFile.exists()) {
                JMeterUtils.loadJMeterProperties(jmeterPropertiesFile.getPath());
                HashTree testPlanTree = new HashTree();
                // create test plan
                TestPlan testPlan = new TestPlan("Create JMeter Script From Java Code");
                // create test sampler
                 JDBCSampler examplecomSampler = new JDBCSampler();// createJDBCSamplerProxy();

                // create loop controller
                LoopController loopController = createLoopController();
                // create thread group
                ThreadGroup threadGroup = createThreadGroup();
                // thread group controller
                threadGroup.setSamplerController(loopController);
                // set plan into the thread group tree
                HashTree threadGroupHashTree = testPlanTree.add(testPlan, threadGroup);
                // add sample to thread group tree
                threadGroupHashTree.add(examplecomSampler);
                // summariser
                Summariser summer = null;
                String summariserName = JMeterUtils.getPropDefault("summariser.name", "summary");
                if (summariserName.length() > 0) {
                    summer = new Summariser(summariserName);
                }
                ResultCollector logger = new ResultCollector(summer);
                testPlanTree.add(testPlanTree.getArray(), logger);

                // jmeter configure
                standardJMeterEngine.configure(testPlanTree);

                System.out.println(TestReadConfig.Thead_Count + " threads is started...");
                // run test
                standardJMeterEngine.run();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * create thread group
     *
     */
    public static ThreadGroup createThreadGroup() {
        ThreadGroup threadGroup = new ThreadGroup();
        threadGroup.setName("Example Thread Group");
        threadGroup.setNumThreads(TestReadConfig.Thead_Count);
        threadGroup.setRampUp(0);
        threadGroup.setProperty(TestElement.TEST_CLASS, ThreadGroup.class.getName());
        threadGroup.setScheduler(false);
        //threadGroup.setDuration(60);
        //threadGroup.setDelay(0);

        /*LoopControlPanel loopPanel = new LoopControlPanel(false);
        LoopController looper = (LoopController) loopPanel.createTestElement();
        looper.setLoops(1);
        threadGroup.setSamplerController(looper);*/
        return threadGroup;
    }

    /**
     * create loop controller
     *
     * @return
     */
    public static LoopController createLoopController() {
        // Loop Controller
        LoopController loopController = new LoopController();
        loopController.setLoops(-1);
        loopController.setContinueForever(true);
        loopController.setProperty(TestElement.TEST_CLASS, LoopController.class.getName());
        loopController.initialize();
        return loopController;
    }

    /**
     * create test sampler
     *
     * @return
     */
    public static JDBCSampler createJDBCSamplerProxy() {
        JDBCSampler sampler = new JDBCSampler();
//        sampler.addTestElement();
        return sampler;
    }

    public static void CreateThreadPool(){
        try {
            hikariDataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
            String connUrl ="jdbc:mysql://"+TestReadConfig.DB_Host+"/"+TestReadConfig.DB_Schema+"?" + "useSSL=false&useUnicode=true&characterEncoding=utf-8&useLegacyDatetimeCode=false&serverTimezone=Asia/Shanghai&allowMultiQueries=true";
            hikariDataSource.setJdbcUrl(connUrl);

            hikariDataSource.setUsername(TestReadConfig.DB_User);

            hikariDataSource.setPassword(TestReadConfig.DB_Password);

//            hikariDataSource.setReadOnly(readOnly);

            hikariDataSource.setConnectionTimeout(30000);

            hikariDataSource.setMinimumIdle(60000);

            hikariDataSource.setMaximumPoolSize(TestReadConfig.Connect_Count);
            System.out.println("Create Conn pool");
        if(TestReadConfig.Insert_Type==2||TestReadConfig.Insert_Type==5||TestReadConfig.Insert_Type==6||TestReadConfig.Insert_Type==7){
            hikariDataSource.addDataSourceProperty("useServerPrepStmts", false);
            hikariDataSource.addDataSourceProperty("rewriteBatchedStatements", true);}
            hikariDataSource.addDataSourceProperty("allowLoadLocalInfile",true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}