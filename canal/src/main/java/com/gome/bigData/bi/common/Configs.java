package com.gome.bigData.bi.common;


import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

/**
 * Created by MaLi on 2017/2/15.
 * 功能: 获取当前程序包中的配置文件
 * 提供Map去get
 */
public class Configs {
    static Properties prop =null;
    static InputStream in = null;
    static {
        prop = new Properties();
        in = Configs.class.getResourceAsStream("/config.properties");
    }
//    public static String get_(String field){
//        String value = null;
//        try {
//            value = PropertiesConfigurable.getInstance().getPropertiesMetaData("classpath:/config/*/*.properties",field);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return value;
//    }

    public static String get(String key){
        //加载配置文件,
        String value = null;
        try {
            prop.load(in);
            value = prop.getProperty(key);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  value;
    }
    public static void main(String[] args){
        String s = get("DataSender_Fen_11_3307.topicName");
        System.out.println(s);
    }
}
