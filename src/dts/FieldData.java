package dts;

import java.util.List;

//字段信息
public class FieldData {
	private String dbName; //数据库名
	private String tableName; // 表名
	private int columnCount; //列的数量
	private List<String> columns; // 列名列表
	private List<Object> values; // 值列表
	private List<String> primaryKey; // 主键名列表，可能存在联合主键
	private List<Object> primaryValue; // 主键值列表

	public FieldData() {
		super();
	}

	public FieldData(String dbName,String tableName,int columnCount, List<String> columns,
			List<Object> values, List<String> primaryKey,
			List<Object> primaryValue) {
		super();
		this.dbName = dbName;
		this.tableName = tableName;
		this.columnCount = columnCount;
		this.columns = columns;
		this.values = values;
		this.primaryKey = primaryKey;
		this.primaryValue = primaryValue;
	}
	
	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public int getColumnCount() {
		return columnCount;
	}

	public void setColumnCount(int columnCount) {
		this.columnCount = columnCount;
	}

	public List<String> getColumns() {
		return columns;
	}

	public void setColumns(List<String> columns) {
		this.columns = columns;
	}

	public List<Object> getValues() {
		return values;
	}

	public void setValues(List<Object> values) {
		this.values = values;
	}

	public List<String> getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(List<String> primaryKey) {
		this.primaryKey = primaryKey;
	}

	public List<Object> getPrimaryValue() {
		return primaryValue;
	}

	public void setPrimaryValue(List<Object> primaryValue) {
		this.primaryValue = primaryValue;
	}
	
}
