package com.gome.bigData.bi.db.relationdb;

import org.apache.commons.dbutils.QueryRunner;

/**
 * Created by MaLi on 2017/1/9.
 */
public abstract class BaseDaoImpl<T> implements IBaseDao<T>{
    protected QueryRunner runner = new QueryRunner(DBUtils.getDataSource());
}
