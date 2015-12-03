package util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;

/**
 * JDBC工具类
 * @author wangsy
 */
public class JDBCUtil {
	//数据源
	private static DataSource ds;
	
	/**
	 * 获取连接
	 * @return Connection
	 */
	public static Connection getConnection() {
		Connection connection = null;
		try {
			DataSource ds = getDataSource(Config.driverName, Config.url,
					Config.userName, Config.password, Config.initialSize, 
					Config.maxSize, Config.maxIdle);
			connection = ds.getConnection();
			return connection;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		} 
	}
	
	public static DataSource getDataSource(String driverName, String url,
			String userName, String password, int initialSize, int maxSize,
			int maxIdle) {
		if (ds == null) {
			BasicDataSource bds = new BasicDataSource();
			bds.setDriverClassName(driverName);
			bds.setUrl(url);
			bds.setUsername(userName);
			bds.setPassword(password);
			bds.setInitialSize(initialSize);
			bds.setMaxTotal(maxSize);
			// 最大空闲连接:连接池中容许保持空闲状态的最大连接数量,超过的空闲连接将被释放,如果设置为负数表示不限制
			bds.setMaxIdle(maxIdle);
			ds = bds;
		}
		return ds;
	}
	
	/**
	 * 统一关闭连接
	 * @param connection Connection
	 */
	public static void closeConnection(ResultSet set,Statement stmt,Connection conn) {
		try {
			if(set!=null){
				set.close();
			}
			if(stmt!=null){
				stmt.close();
			}
			if(null != conn && !conn.isClosed()) {
				conn.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
