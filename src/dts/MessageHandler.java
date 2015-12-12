package dts;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.JDBCUtil;

import com.aliyun.drc.client.message.DataMessage.Record;
import com.aliyun.drc.client.message.DataMessage.Record.Field;
import com.aliyun.drc.client.message.DataMessage.Record.Type;
import com.aliyun.drc.client.message.transfer.String2JavaObject;

//消息处理类
public class MessageHandler {
	private Logger log = LoggerFactory.getLogger(MessageHandler.class);

	// 1、处理新增消息
	public void handleInsert(Record record) {
		Connection conn = JDBCUtil.getConnection();
		PreparedStatement pst = null;
		try {
			conn.setAutoCommit(false);
			FieldData data = getColumnsAndValues(record);
			// 拼接插入语句
			String sql = "insert into " + data.getDbName()+"." + data.getTableName() + " (";
			for (int i = 0; i < data.getColumns().size(); i++) {
				sql += data.getColumns().get(i) + ",";
			}
			sql = sql.substring(0, sql.length() - 1) + " ) values ( ";
			for (int i = 0; i < data.getColumns().size(); i++) {
				sql += " ?,";
			}
			sql = sql.substring(0, sql.length() - 1) + " ) ";
			pst = conn.prepareStatement(sql);

			log.info("insert sql: " + sql);

			// 给占位符赋值
			for (int i = 0; i < data.getValues().size(); i++) {
				pst.setObject(i + 1, data.getValues().get(i));
			}
			pst.execute();
			conn.commit();
		} catch (SQLException e) {
			log.error("insert error:",e);
			try {
				conn.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		} finally {
			JDBCUtil.closeConnection(null, pst, conn);
		}
	}

	// 2、处理修改消息
	public void handleUpdate(Record record) {
		Connection conn = JDBCUtil.getConnection();
		PreparedStatement pst = null;
		try {
			conn.setAutoCommit(false);
			FieldData data = getColumnsAndValues(record);

			// 拼接插入语句
			String sql = "update " + data.getDbName()+"." + data.getTableName() + " set ";
			for (int i = 0; i < data.getColumnCount(); i++) {
				sql += data.getColumns().get(i) + " = ?,";
			}
			sql = sql.substring(0, sql.length() - 1) + "  where 1=1  ";
			for (int i = 0; i < data.getPrimaryKey().size(); i++) {
				sql += " and " + data.getPrimaryKey().get(i) + " = ? ";
			}
			pst = conn.prepareStatement(sql);
			
			log.info("update sql: " + sql);

			// 给占位符赋值
			for (int i = 0; i < data.getValues().size(); i++) {
				pst.setObject(i + 1, data.getValues().get(i));
			}
			for (int i = 0; i < data.getPrimaryValue().size(); i++) {
				pst.setObject(data.getValues().size() + 1, data
						.getPrimaryValue().get(i));
			}
			pst.execute();
			conn.commit();
		} catch (SQLException e) {
			log.error("update error:",e);
			try {
				conn.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		} finally {
			JDBCUtil.closeConnection(null, pst, conn);
		}
	}

	// 3、处理删除消息
	public void handleDelete(Record record) {
		Connection conn = JDBCUtil.getConnection();
		PreparedStatement pst = null;
		try {
			conn.setAutoCommit(false);
			FieldData data = getColumnsAndValues(record);
			// 拼接删除语句
			String sql = "delete from " + data.getDbName()+"." + data.getTableName() + " where 1=1 ";
			for (int i = 0; i < data.getPrimaryKey().size(); i++) {
				sql += " and " + data.getPrimaryKey().get(i) + " = ? ";
			}
			pst = conn.prepareStatement(sql);

			log.info("delete sql: " + sql);

			for (int i = 0; i < data.getPrimaryValue().size(); i++) {
				pst.setObject(i + 1, data.getPrimaryValue().get(i));
			}
			pst.execute();
			conn.commit();
		} catch (SQLException e) {
			log.error("delete error",e);
			try {
				conn.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		} finally {
			JDBCUtil.closeConnection(null, pst, conn);
		}
	}

	// 4、处理ddl消息
	public void handleDdl(Record record) {
		Connection conn = JDBCUtil.getConnection();
		Statement st = null;
		FieldData data = getColumnsAndValues(record);
		if (data.getValues().size() > 0) {
			try {
				conn.setAutoCommit(false);
				st = conn.createStatement();
				//添加操作的数据库
				st.addBatch(" use " + data.getDbName());
				log.info("ddl sql: "+ " use " + data.getDbName() + "\n");
				//添加执行ddl语句
				for (int i = 0; i < data.getValues().size(); i++) {
					String sql = (String) data.getValues().get(i);
					log.info(sql+"\n");
					st.addBatch(sql);
				}
				//批量执行
				st.executeBatch();
				conn.commit();
			} catch (SQLException e) {
				log.error("ddl error", e);
				try {
					conn.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			} finally {
				JDBCUtil.closeConnection(null, st, conn);
			}
		}
	}

	// 解析字段信息
	public FieldData getColumnsAndValues(Record record) {
		String dbName = record.getDbname(); //数据库名
		String tableName = record.getTablename(); //表名
		List<String> columns = new ArrayList<String>(); //列名列表
		List<Object> values = new ArrayList<Object>();  //列值列表
		List<String> primaryKey = new ArrayList<String>(); //主键名列表
		List<Object> primaryValue = new ArrayList<Object>(); //主键值列表

		int columnCount; // 列的数量
		// 当为修改时，每个字段分为修改前的和修改后的
		if (record.getOpt() == Type.UPDATE) {
			columnCount = record.getFieldCount() / 2;
		} else {
			columnCount = record.getFieldCount();
		}
		List<Field> fields = record.getFieldList();
		int i = 0;
		while (i < fields.size()) {
			Field f = null;
			if (record.getOpt() == Type.UPDATE) {
				f = fields.get(i + 1);
			} else {
				f = fields.get(i);
			}
			try {
				String key = f.getFieldname();
				Object value = String2JavaObject.field2Java(f).getObjectValue();
				columns.add(key);
				values.add(value);
				if (f.primaryKey) { // 如果是主键
					primaryKey.add(key);
					primaryValue.add(value);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			if (record.getOpt() == Type.UPDATE) {
				i += 2;
			} else {
				i++;
			}
		}
        
		//封装字段信息对象
		FieldData data = new FieldData(dbName,tableName, columnCount, columns, values,
				primaryKey, primaryValue);
		return data;
	}

}
