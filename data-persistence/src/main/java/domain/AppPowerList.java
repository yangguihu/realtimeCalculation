package domain;

import java.sql.Date;

/**
 * Created by IntelliJ IDEA.
 * User: Seven.Jia
 * Date: 17/1/10
 * Description:
 */
public class AppPowerList extends BaseBean{

    private int id;

    private int powerId;

    private String powerName;

    private int pid;

    private Date updateTime;


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getPowerId() {
        return powerId;
    }

    public void setPowerId(int powerId) {
        this.powerId = powerId;
    }

    public String getPowerName() {
        return powerName;
    }

    public void setPowerName(String powerName) {
        this.powerName = powerName;
    }

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }
}
