package util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

//properties属性文件对应JAVA类
public class Config {
	private static String dbFile ="db.properties";
	public static String accessKey;
	public static String secret;
	public static String guid;
	public static String driverName;// 驱动
	public static String url = null;// 数据库连接地址
	public static String userName = null;// 数据库登录用户
	public static String password = null;// 数据库登录密码
	public static int initialSize = 5;// 初始化连接数
	public static int maxSize = 20;// 最大连接数
	public static int maxIdle = 2;// 最大空闲连接数

	static {
		readConfig();
	}

	private static void readConfig() {
		InputStream is = Config.class.getClassLoader().getResourceAsStream(dbFile);
		Properties pro = new Properties();
		try {
			pro.load(is);
			accessKey = pro.getProperty("accessKey");
			secret = pro.getProperty("secret");
			guid = pro.getProperty("guid");
			driverName = pro.getProperty("driverName");
			url = pro.getProperty("url");
			userName = pro.getProperty("userName");
			password = pro.getProperty("password");
			initialSize = Integer.parseInt(pro.getProperty("initialSize"));
			maxSize = Integer.parseInt(pro.getProperty("maxSize"));
			maxIdle = Integer.parseInt(pro.getProperty("maxIdle"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
