package com.bsb.mmp.java.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by aston on 2017/6/8.
 */
public class Properity {
    private static Properties property;
    static  {
        property =  new  Properties();
        InputStream in = Properity.class.getClassLoader().getResourceAsStream("system.properties");
        try  {
            property.load(in);
            in.close();
        }  catch  (IOException e) {
            e.printStackTrace();
        }
    }


    public static String getProperty(String key) {
        return key == null?null:property.getProperty(key);
    }

    public static String getProperty(String key, String defaultValue) {
        return property.getProperty(key, defaultValue);
    }

    public static Properties getProperty() {
        return property;
    }
}
