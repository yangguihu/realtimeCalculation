package conf;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 * User: Seven.Jia
 * Date: 17/1/10
 * Description:
 */
public class JdbcConfig implements Serializable {

    public String jdbc_driverClassName;
    public String jdbc_url;
    public String jdbc_username;
    public String jdbc_password;



    @Override
    public String toString() {
        return "JdbcConfig{" + "jdbc_driverClassName='" + jdbc_driverClassName + '\''
                + ", jdbc_url='" + jdbc_url + '\''
                + ", jdbc_username='" + jdbc_username + '\''
                + ", jdbc_password='" + jdbc_password + '\'' + '}';
    }

}
