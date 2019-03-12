/****
 */
package util;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.impl.NewProxyPreparedStatement;
import conf.JdbcConfig;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * Created by IntelliJ IDEA.
 * User: Seven.Jia
 * Date: 17/1/9
 * Description:jdbc数据库查询处理
 */
public class JDBCHandler implements Serializable {
    private static final Logger LOG = Logger.getLogger(JDBCHandler.class);
    static Field field = null;

    static {
        try {
            field = NewProxyPreparedStatement.class.getDeclaredField("inner");
            field.setAccessible(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private JdbcConfig jdbcConfig;
    //private SendSmsHandler sendSmsHandler;
    /**
     * 数据库连接
     */
    transient private DataSource dataSource;

    public JDBCHandler(JdbcConfig jdbcConfig) {
        this.jdbcConfig = jdbcConfig;
        init();
    }

    private void init() {
        C3P0DataSourceTemplate dataSourceTemplate = new C3P0DataSourceTemplate(jdbcConfig);
        dataSource = dataSourceTemplate.getDataSource();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject(); // 这里, 由jvm将默认序列化的参数读出来.
        init();
    }

    private Object getPreparedSql(PreparedStatement preparedStatement) {
        if (field != null && preparedStatement != null && preparedStatement instanceof NewProxyPreparedStatement) {
            Object object;
            try {
                return object = field.get(preparedStatement);

            } catch (Exception e) {
                LOG.error("error happened when get inner field", e);
                e.printStackTrace();
            }

        }
        return null;
    }

    public <T> boolean doBatchUpdate(BatchUpdateSqlTemplate<T> handler) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            // connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(handler.getSql());
            Iterator<T> iterator = handler.getUpdateValues().iterator();
            if (handler.batchSize() > 0) {
                int i = 0;
                while (iterator.hasNext()) {
                    T t = iterator.next();
                    try{
                    	handler.setSqlParams(preparedStatement, t);
                    }catch(Exception e){
                    	
                    }
                    
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("the inner is :" + getPreparedSql(preparedStatement));
                    }
                    i++;
                    // 如果达了阀值, 批量提交一次
                    preparedStatement.addBatch();
                    if (i % handler.batchSize() == 0) {
                        preparedStatement.executeBatch();
                    }
                    // preparedStatement.executeUpdate();
                }
                // 最后如果还有未提交的, 批量提交一次
                if (i % handler.batchSize() != 0) {
                    preparedStatement.executeBatch();
                }
            } else { // 不选择批量提交, 就每次都提交
                while (iterator.hasNext()) {
                    T t = iterator.next();
                    handler.setSqlParams(preparedStatement, t);
                    preparedStatement.executeUpdate();
                }
            }
            handler.commit(connection);
        } catch (SQLException e) {
            LOG.error("error happend when batch:" + getPreparedSql(preparedStatement), e);
            try {
                connection.rollback();
            } catch (SQLException e1) {
                LOG.error(e1);
            }
            return false;
        } finally {
            if (null != resultSet) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                }
            }
            if (null != preparedStatement) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                }
            }
            if (null != connection) {
                try {
                    connection.close();
                } catch (SQLException e) {
                }
            }
        }
        return true;
    }

    public boolean doSimpleUpdate(SimpleUpdateSqlTemplate handler) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            // connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(handler.getSql());
            handler.setSqlParams(preparedStatement);
            if (LOG.isDebugEnabled()) {
                LOG.debug("the inner is :" + getPreparedSql(preparedStatement));
            }
            preparedStatement.executeUpdate();
            handler.commit(connection);
        } catch (SQLException e) {
            LOG.error("error happend when simple query:" + getPreparedSql(preparedStatement), e);
            try {
                //sendSmsHandler.sendHttPost(null, "simpleUpd error:" + handler.getClass().getName());
                connection.rollback();
            } catch (Exception e1) {
                LOG.error(e1);
            }
            return false;
        } finally {
            if (null != resultSet) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                }
            }
            if (null != preparedStatement) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                }
            }
            if (null != connection) {
                try {
                    connection.close();
                } catch (SQLException e) {
                }
            }
        }
        return true;
    }

    /**
     * 进行查询操作
     *
     * @param handler
     * @return 操作是否正常处理
     */
    public boolean doQuery(PrepareSqlTemplate handler) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            preparedStatement = connection.prepareStatement(handler.getSql());
            handler.setSqlParams(preparedStatement);
            if (LOG.isDebugEnabled()) {
                LOG.debug("the inner is :" + getPreparedSql(preparedStatement));
            }
            resultSet = preparedStatement.executeQuery();
            handler.process(resultSet);
            handler.commit(connection);
        } catch (SQLException e) {
            LOG.error("query error:" + getPreparedSql(preparedStatement));
            try {
                //sendSmsHandler.sendHttPost(null, "query error:" + handler.getClass().getName());
                connection.rollback();
            } catch (Exception e1) {
                LOG.error(e1);
            }
            return false;
        } finally {
            if (null != resultSet) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                }
            }
            if (null != preparedStatement) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                }
            }
            if (null != connection) {
                try {
                    connection.close();
                } catch (SQLException e) {
                }
            }
        }
        return true;
    }

    public void cleanup() {
        if (dataSource instanceof ComboPooledDataSource) {
            ((ComboPooledDataSource) dataSource).close();
        }
    }

    public interface BaseSqlTemplate {
        /**
         * @return 要查询的sql
         */
        String getSql();

        /**
         * @param connection
         * @throws java.sql.SQLException
         */
        void commit(Connection connection) throws SQLException;
    }

    public static abstract class PrepareSqlTemplate implements BaseSqlTemplate {
        /**
         * 参数设置
         *
         * @param preparedStatement
         * @throws java.sql.SQLException
         */
        public void setSqlParams(PreparedStatement preparedStatement) throws SQLException {

        }

        /**
         * 处理sql查询的结果
         *
         * @param resultSet
         * @return 处理的条目数
         * @throws java.sql.SQLException
         */
        public abstract void process(ResultSet resultSet) throws SQLException;

        @Override
        public void commit(Connection connection) throws SQLException {
            // connection.commit();
        }

    }

    public static abstract class BatchUpdateSqlTemplate<T> implements BaseSqlTemplate {
        /**
         * 参数设置
         *
         * @param preparedStatement
         * @throws java.sql.SQLException
         */
        public abstract void setSqlParams(PreparedStatement preparedStatement, T t) throws SQLException;

        /**
         * 要更新的value
         *
         * @return
         */
        public abstract Iterable<T> getUpdateValues();

        /**
         * 批量更新条目数.
         *
         * @return
         */
        public abstract int batchSize();

        @Override
        public void commit(Connection connection) throws SQLException {
            // connection.commit();
        }
    }

    public static abstract class SimpleUpdateSqlTemplate implements BaseSqlTemplate {
        /**
         * 参数设置
         *
         * @param preparedStatement
         * @throws java.sql.SQLException
         */
        public abstract void setSqlParams(PreparedStatement preparedStatement) throws SQLException;

        @Override
        public void commit(Connection connection) throws SQLException {
            // connection.commit();
        }
    }
}
