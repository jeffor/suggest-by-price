package org.sg.policy;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.sg.data.Item;

public class Update extends Operate {

	private static final SimpleDateFormat fmt = new SimpleDateFormat(
			"YYYY-MM-dd HH:mm:ss");

	public void run(String DBURL, String DBUSER, String DBPWD, String sql)
			throws IOException, SQLException {
		initialize(DBURL, DBUSER, DBPWD); // 初始化：读取配置文件，打开索引文件，连接数据库
		ResultSet rst = null;
		Statement stmt = conn.createStatement();
		Date curDate;
		int start = 0;
		/* 遍历数据库更新item */
		do {
			String query = sql + " where update_time >=  \""
					+ fmt.format(update_time) + "\" limit " + start + ", "
					+ delta;
			System.out.println(query);
			rst = stmt.executeQuery(query);
			/* 遍历数据库，索引数据 */
			while (rst.next())
				updateItem(rst.getInt(idField), rst.getFloat(priceField),
						rst.getTimestamp(dateField));
			rst.last();
			if (rst.getRow() > 0) {
				curDate = rst.getTimestamp(dateField);
				if (curDate == null || curDate.getTime() == update_time)
					start += delta;
				else {
					start = 0;
					update_time = curDate.getTime();
				}
				System.out.println("update data , update_time = "+fmt.format(update_time));
			}
		} while (rst.getRow() == delta);
		writeConfig(); // 更新配置文件
		closeFiles(); // 关闭索引文件和数据库连接
	}

	private void updateItem(int id, float price, Date date) throws IOException {
		Item item = metaFile.read(id);
		if (date.getTime() <= item.update_time)
			return;
		priceFile.addPriceWithUpdateItem(item, price, date);
		metaFile.write(item);
		if (maxId < id)
			maxId = id;
	}
}