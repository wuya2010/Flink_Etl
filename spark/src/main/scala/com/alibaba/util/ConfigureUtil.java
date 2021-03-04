package main.scala.com.alibaba.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/3/3 16:58
 */
public class ConfigureUtil {

    public static Properties prop = new Properties();

    static {
        try {
            //从resource获取数据
            InputStream fis = ConfigureUtil.class.getResourceAsStream("/datasource.properties");
            prop.load(fis);

            //这种方式获取不到数据
//            InputStreamReader inputStreamReader = new InputStreamReader(ConfigureUtil.class.getResourceAsStream("/datasource.properties"));
//            prop.load(inputStreamReader);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key){
        return prop.getProperty(key);
    }

    public static void main(String[] args) {
        System.out.println(getProperty("hbase.zookeeper.quorum"));
    }

}
