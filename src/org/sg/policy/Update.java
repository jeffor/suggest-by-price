package org.sg.policy;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import org.sg.data.Item;
import org.sg.util.ItemSet;

public class Update extends Operate {

	public void run(String DBURL, String DBUSER, String DBPWD, String sql,
			ItemSet set, long from) throws IOException, SQLException {
		initialize(DBURL, DBUSER, DBPWD); // 初始化：读取配置文件，打开索引文件，连接数据库
		ResultSet rst = null;
		Statement stmt = conn.createStatement();
		if (from > 0)
			maxId = from;
		/* 遍历数据库更新item */
		do {
			String query = sql + " where id >  " + maxId + " limit " + delta;
			System.out.println("update items: " + query);
			rst = stmt.executeQuery(query);
			/* 遍历数据库，索引数据 */
			while (rst.next())
				try {
					updateItem(rst.getInt(itemIdField),
							rst.getFloat(priceField),
							rst.getTimestamp(dateField), set);
				} catch (Exception e) {
					e.printStackTrace();
					System.out.format(
							"itemid = %d filePoint = %d fileLength = %d \n",
							rst.getInt(itemIdField), metaFile.pointer(),
							metaFile.length());
				}
			rst.last();
			if (rst.getRow() > 0)
				maxId = rst.getInt(idField);
		} while (rst.getRow() == delta);
		writeConfig(); // 更新配置文件
		System.out.println("maxid = " + maxId);
		closeFiles(); // 关闭索引文件和数据库连接
	}

	private void updateItem(int id, float price, Date date, ItemSet set)
			throws IOException {
		Item item = metaFile.read(id);
		if (date != null && price != 0 && date.getTime() > item.update_time) {
			priceFile.writePrice(item.id, item.elemLength, price); // 写入新价格
			priceFile.updateItem(item, price, date.getTime());
			metaFile.write(item);
			set.add(item, 0);
		}
	}
}