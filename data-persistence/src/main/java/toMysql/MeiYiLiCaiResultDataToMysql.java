//package toMysql;
//
//import domain.AppPowerList;
//import domain.BaseBean;
//import util.JDBCHandler;
//
//import java.sql.PreparedStatement;
//import java.sql.SQLException;
//import java.util.Arrays;
//
///**
// * Created by IntelliJ IDEA.
// * User: Seven.Jia
// * Date: 17/1/10
// * Description:
// */
//public class MeiYiLiCaiResultDataToMysql extends BaseResultDataToMysql{
//
//    public void prepare(BaseBean baseBean) {
//        super.prepare(null);
//        AppPowerList[] list = new AppPowerList[1];
//        list[0] = (AppPowerList) baseBean;
//        app_power_list(list);
//    }
//
//    private void app_power_list(final AppPowerList[] list) {
//        JDBCHandler.BatchUpdateSqlTemplate<AppPowerList> insertIntoAppPowerList = new JDBCHandler.BatchUpdateSqlTemplate<AppPowerList>() {
//            @Override
//            public String getSql() {
//                return "insert into app_power_list" +
//                        "(id                       "
//                        + ",power_id                    "
//                        + ",power_name                   "
//                        + ",pid                   "
//                        + ",update_time                ) "
//                        + " values(?,?,?,?"
//                        + ",CURRENT_TIMESTAMP);";
//
//            }
//            @Override
//            public void setSqlParams(PreparedStatement preparedStatement, AppPowerList obj) throws SQLException {
//                preparedStatement.setInt(1, obj.getId());
//                preparedStatement.setInt(2, obj.getPowerId());
//                preparedStatement.setString(3, obj.getPowerName());
//                preparedStatement.setInt(4, obj.getPid());
//            }
//
//            @Override
//            public Iterable<AppPowerList> getUpdateValues() {
//                return Arrays.asList(list);
//            }
//
//            @Override
//            public int batchSize() {
//                return 1;
//            }
//        };
//        jdbcHandler.doBatchUpdate(insertIntoAppPowerList);
//
//    }
//
//}
