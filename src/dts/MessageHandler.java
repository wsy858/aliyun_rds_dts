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
	public boolean handleInsert(Record record) {
		boolean finished = false; //是否完成
		Connection conn = JDBCUtil.getConnection();
		PreparedStatement pst = null;
		try {
			conn.setAutoCommit(false);
			FieldData data = getColumnsAndValues(record);
			// 拼接插入语句
			String sql = "insert into " + data.getTableName() + " (";
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
			finished = true;
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
		return finished;
	}

	// 2、处理修改消息
	public boolean handleUpdate(Record record) {
		boolean finished = false; //是否完成
		Connection conn = JDBCUtil.getConnection();
		PreparedStatement pst = null;
		try {
			conn.setAutoCommit(false);
			FieldData data = getColumnsAndValues(record);

			// 拼接插入语句
			String sql = "update " + data.getTableName() + " set ";
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
			finished = true;
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
		return finished;
	}

	// 3、处理删除消息
	public boolean handleDelete(Record record) {
		boolean finished = false; //是否完成
		Connection conn = JDBCUtil.getConnection();
		PreparedStatement pst = null;
		try {
			conn.setAutoCommit(false);
			FieldData data = getColumnsAndValues(record);
			// 拼接删除语句
			String sql = "delete from " + data.getTableName() + " where 1=1 ";
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
			finished = true;
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
		return finished;
	}

	// 4、处理ddl消息
	public boolean handleDdl(Record record) {
		boolean finished = false; //是否完成
		Connection conn = JDBCUtil.getConnection();
		Statement st = null;
		FieldData data = getColumnsAndValues(record);
		if (data.getValues().size() > 0) {
			try {
				conn.setAutoCommit(false);
				st = conn.createStatement();
				//执行ddl语句
				for (int i = 0; i < data.getValues().size(); i++) {
					String sql = (String) data.getValues().get(i);
					log.info("ddl sql: " + sql);
					st.execute(sql);
				}
				conn.commit();
				finished = true;
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
		return  finished;
	}

	// 解析字段信息
	public FieldData getColumnsAndValues(Record record) {
		String tableName = record.getTablename();
		List<String> columns = new ArrayList<String>();
		List<Object> values = new ArrayList<Object>();
		List<String> primaryKey = new ArrayList<String>();
		List<Object> primaryValue = new ArrayList<Object>();

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
		FieldData data = new FieldData(tableName, columnCount, columns, values,
				primaryKey, primaryValue);
		return data;
	}

}
