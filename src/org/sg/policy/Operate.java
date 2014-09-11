package org.sg.policy;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.sg.data.MetaFile;
import org.sg.data.PriceFile;

public class Operate {
	
	protected static final String idField = "id";
	protected static final String priceField = "price";
	protected static final String itemIdField = "item_id";
	protected static final String dateField = "update_time";
	protected static final String mysqlClass = "com.mysql.jdbc.Driver";
	protected static final int delta = 20000; // 数据库读取缓存量
	protected static final String cfgFileName = "./lib/suggest.cfg";
	
	protected Connection conn; // 数据库连接
	protected RandomAccessFile cfgFile; // 配置文件
	protected MetaFile metaFile = new MetaFile();
	protected PriceFile priceFile = new PriceFile();

	protected long maxId;
	protected long update_time;


	/** 初始化加载数据库驱动 */
	public Operate() {
		try {
			Class.forName(mysqlClass);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	/* 写配置 */
	protected void writeConfig() throws IOException {
		cfgFile.seek(0);
		cfgFile.writeLong(update_time);
		cfgFile.writeLong(maxId);
	}

	/* 初始化数据库连接，打开索引文件 */
	protected void initialize(String DBURL, String DBUSER, String DBPWD)
			throws SQLException, IOException {
		conn = DriverManager.getConnection(DBURL, DBUSER, DBPWD);
		openFiles();
	}

	/* 打开索引文件 */
	protected void openFiles() throws IOException {
		cfgFile = new RandomAccessFile(new File(cfgFileName), "rw");
		if(cfgFile.length() > 0){
			update_time = cfgFile.readLong()-60*60*1000;
			maxId = cfgFile.readLong();
		}
		else{
			update_time = maxId = -1;
		}
	}

	/* 关闭索引文件和数据库连接 */
	protected void closeFiles() throws IOException, SQLException {
		metaFile.closeFiles();
		priceFile.closeFiles();
		cfgFile.close();
		if (conn != null)
			conn.close();
		conn = null;
	}
}