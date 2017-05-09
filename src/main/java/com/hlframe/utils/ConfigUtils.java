package com.hlframe.utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

/**
 * @类名: ConfigUtils
 * @职责说明: use for get config properties
 * @创建者: Primo
 * @创建时间: 2017/4/6
 */
public class ConfigUtils {
    public static final String dcPropPath = "config.properties";
    static Properties dcPropertie;

    //最后修改时间
    static long lastTime;

    static{
        lastTime = new File(dcPropPath).lastModified();
        System.out.println("---->"+lastTime);
        reloadProperTies();
    }

    /**
     * @方法名称: reloadProperTies
     * @职责说明: 重新加载配置文件
     * @参数:
     * @返回: void
     * @创建者: pjd
     * @创建时间: 2016年11月3日 下午3:35:13
     */
    public static void reloadProperTies() {
        try {
            dcPropertie = load(dcPropPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @方法名称: getProperty
     * @职责说明: 读取配置文件
     * @参数: @param propName	参数名
     * @参数: @param defaultVal 默认值
     * @参数: @return
     * @返回: String
     * @创建者: pjd
     * @创建时间: 2016年11月3日 下午3:41:10
     */
    public static String getProperty(String propName, String defaultVal) {
        //如果配置文件发生修改, 则重新加载
        if (lastTime!=new File(dcPropPath).lastModified()) {
            reloadProperTies();
        }
        if (null!=dcPropertie) {
            return dcPropertie.getProperty(propName, defaultVal);
        }
        return defaultVal;
    }

    /**
     * @方法名称: getProperty
     * @实现功能: 获取配置文件默认属性
     * @param propName
     * @return
     * @create by peijd at 2016年11月24日 下午6:46:40
     */
    public static String getProperty(String propName) {
        //如果配置文件发生修改, 则重新加载
        if (lastTime!=new File(dcPropPath).lastModified()) {
            reloadProperTies();
        }
        if (null!=dcPropertie) {
            return dcPropertie.getProperty(propName);
        }
        return null;
    }

    /**
     * @方法名称: load
     * @实现功能: 载入Properties 对象
     * @param path
     * @return
     * @throws IOException
     * @create 2016年11月3日 下午3:45:08
     */
    public static Properties load(String path) throws IOException {
        InputStream is = ConfigUtils.class.getClassLoader().getResourceAsStream(path);
//        InputStream is = this.getClass().getResource("/").getPath().getResourceAsStream(path);
        if(is == null){
            throw new IOException(path+" file is not found!");
        }
        try {
            return load(is);
        } finally {
            is.close();
        }
    }

    /**
     * @方法名称: load
     * @实现功能: 根据文件流 载入Properties 对象
     * @param is
     * @return
     * @throws IOException
     * @create by peijd at 2016年11月3日 下午3:41:13
     */
    public static Properties load(InputStream is) throws IOException {
        Properties prop = new Properties();
        prop.load(is);

        // 转码处理
        Set<Object> keyset = prop.keySet();
        Iterator<Object> iter = keyset.iterator();
        while (iter.hasNext()) {
            String key = (String) iter.next();
            String newValue = null;
            try {
                // 属性配置文件自身的编码
                newValue = new String(prop.getProperty(key).getBytes("ISO-8859-1"), "utf-8");
            } catch (UnsupportedEncodingException e) {
                newValue = prop.getProperty(key);
            }
            prop.setProperty(key, newValue);
        }

        return prop;
    }

    public static void main(String[] args) {
        System.out.println("-->"+ConfigUtils.class.getResource("/").getPath());
        System.out.println(getProperty("elasticSearch.main.url", "AA"));
        System.out.println(getProperty("etl.dbconfig.path", "BB"));
    }
}
