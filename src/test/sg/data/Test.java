package test.sg.data;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Collection;
import java.util.Date;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.sg.data.SolrApi;
import org.sg.policy.Initialize;
import org.sg.policy.MergeWeight;
import org.sg.policy.TopK;
import org.sg.policy.Update;
import org.sg.util.ItemSet;

public class Test {

	private static SolrApi solr = new SolrApi();

	/**
	 * 初始化 主要实现历史价格采集 和 计算结构的建立
	 * 
	 * @param from
	 *            起始id
	 * @return void
	 * */
	public static void init(int from) throws IOException, SQLException {
		Initialize init = new Initialize();
		init.run("jdbc:mysql://192.168.1.30:3306/extension", "root",
				"4rfv&UJM", "select * from site_item_price", from);
		// init.run("jdbc:mysql://192.168.3.107:3306/test", "user", "pwd",
		// "select * from site_item_price", from);
	}

	/**
	 * 合并历史价格 计算 元数据
	 * 
	 * @param startId
	 *            起始数据id
	 * */
	public static void merge(int startId) throws IOException, SQLException {
		MergeWeight mer = new MergeWeight();
		mer.run(startId);
	}

	/**
	 * 更新 检索引擎数据
	 * 
	 * @param url
	 *            检索引擎地址
	 * */
	public static void update(String url) throws IOException, SQLException,
			InterruptedException, ParseException, NumberFormatException,
			SolrServerException {
		ItemSet set = upload(url); // 更新索引并 TopN 堆
		Update update = new Update(); // 初始化数据更新对象
		while (true) {
			set.clear();
			update.run("jdbc:mysql://192.168.1.30:3306/extension", "root",
					"4rfv&UJM", "select * from site_item_price", set, 0);
			// update.run("jdbc:mysql://192.168.3.107:3306/test", "user", "pwd",
			// "select * from site_item_price", heap);
			upload(set, url);
			Thread.sleep(1000 * 60 * 60);
		}
	}

	public static void pureUpdate(String url, long from) throws IOException, SQLException,
			InterruptedException, ParseException, NumberFormatException,
			SolrServerException {
		ItemSet set = new ItemSet(); // 更新索引并 TopN 堆
		Update update = new Update(); // 初始化数据更新对象
		while (true) {
			set.clear();
			update.run("jdbc:mysql://192.168.1.30:3306/extension", "root",
					"4rfv&UJM", "select * from site_item_price", set, from);
			// update.run("jdbc:mysql://192.168.3.107:3306/test", "user", "pwd",
			// "select * from site_item_price", heap);
			upload(set, url);
			from = 0;
			Thread.sleep(1000 * 60 * 60);
		}
	}

	/**
	 * 上传 topN 数据至检索引擎
	 * 
	 * @param num
	 *            N值
	 * @param url
	 *            检索引擎地址
	 * @return heap topN堆
	 * */
	private static ItemSet upload(String url) throws ParseException,
			NumberFormatException, SolrServerException, IOException,
			SQLException {
		long baseTime = System.currentTimeMillis() - 24 * 60 * 60 * 1000;
		TopK top = new TopK();
		top.run(baseTime);
		upload(top.getSet(), url);
		return top.getSet();
	}

	/**
	 * 将堆中的数据上传至检索引擎
	 * 
	 * @throws ParseException
	 * @throws SQLException
	 */
	private static void upload(ItemSet set, String url)
			throws NumberFormatException, SolrServerException, IOException,
			ParseException, SQLException {
		System.out.println("sync time = " + new Date());
		Collection<SolrInputDocument> docs = solr.readDocs(set, "root",
				"4rfv&UJM", "jdbc:mysql://192.168.1.30:3306/extension");
		// Collection<SolrInputDocument> docs = solr.readDocs(heap.iterator(),
		// fmt
		// .parse(date).getTime(), "user", "pwd",
		// "jdbc:mysql://192.168.3.107:3306/test");
		if (docs.size() > 0) {
			solr.connectServer(url);
			solr.upload(docs);
			solr.closeConnection();
		}
		System.out.println("import indexDoc number = " + docs.size());
	}

	/**
	 * 清空指定索引
	 * 
	 * @param url
	 *            指定索引url
	 * @return void
	 * */
	public static void clearIndex(String url) throws SolrServerException,
			IOException {
		solr.connectServer(url);
		solr.clearIndex();
		solr.closeConnection();
	}

	public static void main(String[] argvs) throws NumberFormatException,
			IOException, SQLException, InterruptedException, ParseException,
			SolrServerException {
		if (argvs[0].equals("-init")) {
			init(Integer.parseInt(argvs[1]));
		} else if (argvs[0].equals("-merge")) {
			merge(Integer.parseInt(argvs[1]));
		} else if (argvs[0].equals("-update")) {
			update("http://192.168.1.30:8080/solr/suggest_system");
		} else if (argvs[0].equals("-pupdate")) {
			pureUpdate("http://192.168.1.30:8080/solr/suggest_system", Integer.parseInt(argvs[1]));
		} else if (argvs[0].equals("-server")) {
			init(Integer.parseInt(argvs[1]));
			merge(Integer.parseInt(argvs[2]));
			update("http://192.168.1.30:8080/solr/suggest_system");
		} else if (argvs[0].equals("-clear")) {
			clearIndex("http://192.168.1.30:8080/solr/suggest_system");
		}

		// init(0);
		// merge(0);
		// select(100, "2010-01-15 00:00:00");
		// clearIndex("http://192.168.1.30:8080/solr/suggest_system");
		// update("http://192.168.1.30:8080/solr/suggest_system");
		// upload(100, "http://192.168.1.30:8080/solr/suggest_system");
	}

}
