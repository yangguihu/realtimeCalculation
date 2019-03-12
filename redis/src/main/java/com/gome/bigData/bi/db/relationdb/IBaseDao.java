package com.gome.bigData.bi.db.relationdb;

import java.sql.SQLException;

/**
 * Created by MaLi on 2017/1/9.
 */
public interface IBaseDao<T> {
    /**
     * 保存一个对象
     * @param t
     */
    public void save(T t) throws SQLException;

    /**
     * 删除一个对象
     * @param id
     */
    public void delete(Long id) throws SQLException;

    /**
     * 修改一个对象
     * @param t
     */
    public void update(T t) throws SQLException;

    /**
     * 查询一个记录
     * @param id
     * @param <T>
     * @return
     */
    public <T> T query(Long id) throws SQLException;
}
