package com.xuebotao.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URLEncoder;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import static org.apache.commons.codec.digest.MessageDigestAlgorithms.MD5;


public class VisitGaode {
    // 对Map内所有value作utf8编码，拼接返回结果
    private static String toQueryString(Map<?, ?> data) throws UnsupportedEncodingException {
        StringBuffer queryString = new StringBuffer();
        for (Map.Entry<?, ?> pair : data.entrySet()) {
            queryString.append(pair.getKey() + "=");
            queryString.append(URLEncoder.encode((String) pair.getValue(), "UTF-8") + "&");
        }
        if (queryString.length() > 0) {
            queryString.deleteCharAt(queryString.length() - 1);
        }
        return queryString.toString();
    }

    // 来自stackoverflow的MD5计算方法，调用了MessageDigest库函数，并把byte数组结果转换成16进制
    private static String MD5(String md5) throws NoSuchAlgorithmException {
        java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
        byte[] array = md.digest(md5.getBytes());
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < array.length; ++i) {
            sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100).substring(1, 3));
        }
        return sb.toString();
    }
    private static String sn(String paramsStr) {
        try {
            // 对paramsStr前面拼接上/geocoder/v2/?，后面直接拼接yoursk得到/geocoder/v2/?address=%E7%99%BE%E5%BA%A6%E5%A4%A7%E5%8E%A6&output=json&ak=yourakyoursk
            String wholeStr = new String("/geocoder/v2/?" + paramsStr + "51d1cbc120d24f1150b015b1ca1405c5");
            //51d1cbc120d24f1150b015b1ca1405c5
            // 对上面wholeStr再作utf8编码
            String tempStr = URLEncoder.encode(wholeStr, "UTF-8");
            return MD5(tempStr);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }



    // 将经纬度getLng， getLat   通过getAmapByLngAndLat方法转换地址
    public static String bussinessTagFromBaidu(String latAndLng) {
        // 最终需要返回的标签
        StringBuffer businessTags = new StringBuffer();
        HttpClient httpClient = new HttpClient();
        GetMethod method = null;
        try {
            String head="https://restapi.amap.com/v3/geocode/regeo?output=json&location=";
            String end="&key=51d1cbc120d24f1150b015b1ca1405c5&radius=1000&extensions=all";

            String finalURL =head+latAndLng+end;
            System.out.println(finalURL);
            //  logger.debug(finalURL);
            method = new GetMethod(finalURL);
            int i = httpClient.executeMethod(method);
            if (i == 200) {
                String bodyAsString = method.getResponseBodyAsString();
                System.out.println(bodyAsString);
                if (!bodyAsString.startsWith("{")) {
                    bodyAsString = bodyAsString.replace("renderReverse&&renderReverse(", "");
                    bodyAsString = bodyAsString.substring(0, bodyAsString.length() - 1);
                }
                // logger.debug(bodyAsString);
                // json 解析结果数据, 解析出来bussiness
                JSONObject parseObject = JSONObject.parseObject(bodyAsString);
                System.out.println(parseObject);
                int status = parseObject.getIntValue("status");
                System.out.println(status);
                if (status == 1) {
                    System.out.println("55555555555");
                    JSONObject result = parseObject.getJSONObject("regeocode");
                    System.out.println(result);
                     JSONArray jsonArray = result.getJSONArray("pois");
                    /* System.out.println(jsonArray);
                    JSONObject object=jsonArray.getJSONObject(0);
                    businessTags.append(object.getString("businessarea"));*/
                    for (int j =0;j<jsonArray.size();j++){
                         JSONObject object = jsonArray.getJSONObject(j);
                         businessTags.append(object.getString("name"));
                         businessTags.append(",");
                    }
                }
            }
        } catch (Exception e) {
            //logger.error(e.fillInStackTrace() + e.getMessage());
        } finally {
            if (null!= method) {
                method.releaseConnection();
            }
        }
        return businessTags.toString();
    }

    public static void main(String[] args) {

        final String ss = bussinessTagFromBaidu("116.310003,39.991957");
        System.out.println("sss");
        System.out.println(ss);
    }
}