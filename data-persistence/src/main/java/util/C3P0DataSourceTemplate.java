package util;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import conf.JdbcConfig;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;

/**
 * 以c3p0做为数据库连接池
 */
public class C3P0DataSourceTemplate {
    private static final Logger LOG = Logger.getLogger(C3P0DataSourceTemplate.class);
    private DataSource dataSource;

    public C3P0DataSourceTemplate(JdbcConfig config) {

        ComboPooledDataSource temp = new ComboPooledDataSource();
        //下列这些从配置信息dataSourceProperties 中获取
        try {
            temp.setDriverClass(config.jdbc_driverClassName);
        } catch (PropertyVetoException e) {
            LOG.error("error when create database , config.c3p0_driver_class: " + config.jdbc_driverClassName);
            throw new RuntimeException(e);
        }
        temp.setJdbcUrl(config.jdbc_url);
        temp.setUser(config.jdbc_username);
        temp.setPassword(config.jdbc_password);
        temp.setAutoCommitOnClose(true);
        temp.setCheckoutTimeout(10000);
        temp.setInitialPoolSize(15);
        temp.setMinPoolSize(5);
        temp.setMaxPoolSize(15);
        temp.setMaxIdleTime(6000);
        temp.setAcquireIncrement(1);
        temp.setMaxIdleTimeExcessConnections(18000);
        temp.setTestConnectionOnCheckout(true);
        temp.setTestConnectionOnCheckin(true);
        temp.setAcquireRetryAttempts(5);
        temp.setAcquireRetryDelay(5000);
        temp.setMaxStatementsPerConnection(50000);
        dataSource = temp;
    }

    public DataSource getDataSource() {
        return dataSource;
    }
}
