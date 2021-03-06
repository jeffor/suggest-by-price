package org.sg.policy;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
//import java.text.SimpleDateFormat;
import java.util.Date;

import org.sg.data.Item;

public class Initialize extends Operate {

	/**
	 * 根据数据库更新数据
	 * 
	 * @param DBURL
	 *            数据库地址
	 * @param DBUSER
	 *            数据库用户名
	 * @param DBPWD
	 *            数据库密码
	 * @param sql
	 *            数据库查询语句
	 * @return boolean 返回更新成功状态
	 * @throws SQLException
	 */
	public void run(String DBURL, String DBUSER, String DBPWD, String sql,
			int from) throws IOException, SQLException {
		initialize(DBURL, DBUSER, DBPWD); // 初始化：读取配置文件，打开索引文件，连接数据库
		ResultSet rst = null;
		Statement stmt = conn.createStatement();
		/* 遍历数据库更新item */
		maxId = from > 0 ? from : 0;
		do {
			String query = sql + " where id >  " + maxId + " limit " + delta;
			System.out.println(query);
			rst = stmt.executeQuery(query);
			/* 遍历数据库，索引数据 */
			while (rst.next())
				try {
					updateItem(rst.getInt(itemIdField),
							rst.getFloat(priceField),
							rst.getTimestamp(dateField));
				} catch (Exception e) {
					e.printStackTrace();
					System.err.println("itemid = " + rst.getInt(itemIdField)
							+ "price = " + rst.getFloat(priceField)
							+ "create_time = " + rst.getTimestamp(dateField));
				}
			rst.last();
			if (rst.getRow() > 0)
				maxId = rst.getInt(idField);
		} while (rst.getRow() == delta);
		writeConfig(); // 更新配置文件
		System.out.println("maxid = " + maxId);
		closeFiles(); // 关闭索引文件和数据库连接
	}

	private void updateItem(int id, float price, Date date) throws IOException {
		Item item = metaFile.read(id);
		if (price != 0 && date != null
				&& date.getTime() > item.update_time) {
			priceFile.writePrice(id, item.elemLength++, price);
			item.update_time = date.getTime();
			metaFile.write(item);
			if (maxItemid < id)
				maxItemid = id;
		}
	}
}