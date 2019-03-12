//package toMysql;
//
//import com.gome.bigData.bi.util.PropertiesConfigurable;
//import conf.JdbcConfig;
//import domain.AppPowerList;
//import domain.BaseBean;
//import org.apache.log4j.Logger;
//import util.JDBCHandler;
//
//import java.io.IOException;
//import java.util.Random;
//import java.util.concurrent.atomic.AtomicInteger;
//
///**
// * Created by IntelliJ IDEA.
// * User: Seven.Jia
// * Date: 17/1/10
// * Description:
// */
//public class BaseResultDataToMysql {
//
//    private static final Logger LOG = Logger.getLogger(BaseResultDataToMysql.class);
//    private AtomicInteger seriaNum;
//    protected JDBCHandler jdbcHandler;
//    protected JdbcConfig jdbcConfig;
//
//    public void prepare(BaseBean baseBean) {
//        try {
//            jdbcConfig = (JdbcConfig)PropertiesConfigurable.getInstance().getPropertiesMetaData("classpath:/config/*/*.properties",new JdbcConfig());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        jdbcHandler = new JDBCHandler(jdbcConfig);
//        seriaNum = new AtomicInteger(new Random().nextInt(1000));
//    }
//    public static void main(String[] args) {
//        BaseResultDataToMysql b = new MeiYiLiCaiResultDataToMysql();
//        AppPowerList ap = new AppPowerList();
//        ap.setId(21);
//        ap.setPid(100);
//        ap.setPowerId(30);
//        ap.setPowerName("haha");
//        b.prepare(ap);
//
//    }
//
//
//
//
//}
