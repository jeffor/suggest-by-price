package org.sg.data;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.sg.policy.Operate;
import org.sg.util.ItemSet;

public class SolrApi extends Operate {
	public HttpSolrServer server; // http服务器 操作句柄
	private final  static int timeZoneDelta = 8*1000*60*60; 

	/**
	 * 连接服务器
	 * 
	 * @param url
	 *            (String)
	 * @return boolean
	 * */
	public boolean connectServer(String url) {
		if (server != null)
			return false;
		server = new HttpSolrServer(url);
		return true;
	}

	/**
	 * 断开服务器连接
	 * 
	 * @param void
	 * @return SolrApi
	 * */
	public SolrApi closeConnection() {
		if (server != null)
			server = null;
		return this;
	}

	/** 清空索引 */
	public void clearIndex() throws SolrServerException, IOException {
		if (server != null) {
			server.deleteByQuery("*:*");
			server.commit();
		}
	}

	/**
	 * 将动态数组转换成上传集合
	 * 
	 * @throws SQLException
	 */
	public Collection<SolrInputDocument> readDocs(ItemSet set, String DBUSER,
			String DBPWD, String DBURL) throws NumberFormatException,
			IOException, SQLException {
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		initialize(DBURL, DBUSER, DBPWD);
		PreparedStatement stmt = conn
				.prepareStatement("select * from site_item where id = ?");
		ResultSet rst = null;
		Item[] items = set.iterator();
		Item item = null;
		for (int index = 0; index < set.length; ++index) {
			item = items[index];
			stmt.setInt(1, item.id);
			rst = stmt.executeQuery();
			if (rst.next()) {
				SolrInputDocument doc = new SolrInputDocument();
				doc.addField("id", item.id);
				doc.addField("title", rst.getString("title"));
				doc.addField("url", rst.getString("source_url"));
				doc.addField("price", item.current_pice);
				doc.addField("weight", item.weight);
				doc.addField("update_time", new Date(item.update_time+timeZoneDelta));
				docs.add(doc);
			}
			else
				System.out.println("sql for site_item error id = "+item.id);
		}
		closeFiles();
		return docs;
	}

	/** 上传数据 */
	public void upload(Collection<SolrInputDocument> doc)
			throws SolrServerException, IOException {
		server.add(doc.iterator());
		server.commit();
	}
}