//package com.gome.bigData.bi.util;
//
//import org.apache.log4j.Logger;
//import org.springframework.core.io.Resource;
//import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
//import org.springframework.core.io.support.ResourcePatternResolver;
//
//import java.io.*;
//import java.lang.reflect.Field;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//import java.util.Properties;
//
///**
// * Created by IntelliJ IDEA.
// * User: Seven.Jia
// * Date: 17/1/10
// * Description:
// */
//public class PropertiesConfigurable {
//    private static final Logger LOG = Logger.getLogger(PropertiesConfigurable.class);
//
//    volatile private static PropertiesConfigurable singleton = null;
//
//    private  static final Map PROPERTIES_MAP = new HashMap();
//
//    private PropertiesConfigurable(){};
//
//    public static PropertiesConfigurable getInstance(){
//        if (null==singleton){
//            synchronized(PropertiesConfigurable.class){
//                if(null == singleton){
//                    singleton = new PropertiesConfigurable();
//                }
//            }
//        }
//        return singleton;
//    }
//
//    private String location = "classpath:/config/*/*.properties";
//
//    public Object getPropertiesMetaData(String location,Object obj) throws IOException {
//        ResourcePatternResolver resourceLoader = new PathMatchingResourcePatternResolver();
//        Resource[] source = resourceLoader.getResources(location);
//        Properties prop = new Properties();
//        for (int i = 0; i < source.length; i++) {
//            Map<String,String> map = new HashMap<>();
//            Resource resource = source[i];
//            File file = resource.getFile();
//            //读取属性文件*.properties
//            InputStream in = new BufferedInputStream(new FileInputStream(file));
//            prop.load(in);     ///加载属性列表
//            Iterator<String> it=prop.stringPropertyNames().iterator();
//            while(it.hasNext()){
//                String key=it.next();
//                map.put(key, prop.getProperty(key));
//            }
//            getByReflect(obj,map);
//        }
//        return obj;
//    }
//
//    //反射获取所有属性值
//    public static Object getByReflect(Object object,Map<String,String> propertiesDataMap){
//        //反射获取所有属性并赋值
//        Field[] f = object.getClass().getDeclaredFields();
//        for(int i=0;i<f.length;i++){
//            Field fd = f[i];
//            fd.setAccessible(true);
//            try {
//                String fieldName = fd.getName();
//                if(null!=propertiesDataMap.get(fieldName)){
//                    fd.set(object,propertiesDataMap.get(fieldName));
//                }
//            } catch (IllegalAccessException e6) {
//                e6.printStackTrace();
//            }
//        }
//        return object;
//    }
//
//    /**
//     * 获取指定配置项的value
//     * @param location
//     * @param field
//     * @return
//     * @throws IOException
//     */
//    public String getPropertiesMetaData(String location,String field) throws IOException {
//        ResourcePatternResolver resourceLoader = new PathMatchingResourcePatternResolver();
//        Resource[] source = resourceLoader.getResources(location);
//        Properties prop = new Properties();
//        String value = null;
//        for (int i = 0; i < source.length; i++) {
//            Map<String,String> map = new HashMap<>();
//            Resource resource = source[i];
//            File file = resource.getFile();
//            //读取属性文件*.properties
//            InputStream in = new BufferedInputStream(new FileInputStream(file));
//            prop.load(in);     ///加载属性列表
//            Iterator<String> it=prop.stringPropertyNames().iterator();
//            while(it.hasNext()){
//                String key=it.next();
//                map.put(key, prop.getProperty(key));
//            }
//            value = map.get(field);
//        }
//        return value;
//    }
//    public static void main(String[] args) {
////        try {
////            Map<String, String> propertiesMetaData = PropertiesConfigurable.getInstance().getPropertiesMetaData("classpath:/config/*/*.properties",);
////            System.out.println("dd");
////        } catch (IOException e) {
////            e.printStackTrace();
////        }
//
//    }
//}
