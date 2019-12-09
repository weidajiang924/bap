package com.utils;

import java.util.ResourceBundle;

class DmpRedisUtil {
    private static ResourceBundle properties;
    static {
        properties = ResourceBundle.getBundle(CommonData.PROPERTEIS_FILE);
    }

    static String getValueByKey(String key) {
        return properties.getString(key);
    }

    static int getIntValueByKey(String key) {
        return Integer.parseInt(properties.getString(key));
    }

    //字符串转为boolean值，切记不能使用Boolean.getBoolean
    static boolean getBooleanValueByKey(String key) {
        return Boolean.parseBoolean(properties.getString(key));
    }
}
