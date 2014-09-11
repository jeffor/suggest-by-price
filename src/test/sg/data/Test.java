package test.sg.data;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.sg.data.Item;
import org.sg.policy.Initialize;
import org.sg.policy.MergeWeight;
import org.sg.policy.TopK;
import org.sg.policy.Update;

public class Test {
	public static void main(String[] argvs) throws SQLException, IOException,
			NumberFormatException, ParseException, InterruptedException {
//		 /*
		if (argvs[0].equals("-init")) {
			init(Integer.parseInt(argvs[1]));
		} else if(argvs[0].equals("-merge")){
			merge();
		}else if(argvs[0].equals("-server")){
			init(Integer.parseInt(argvs[1]));
			merge();
			while(true){
				Thread.sleep(1000*60*60);
				update();
			}
		}else
			select(Integer.parseInt(argvs[0]), argvs[1]+" "+argvs[2]);
//		 */
//		 init(0);
//		 merge();
//		 select(100,"2010-01-15 00:00:00");
//		update();
	}

	public static void init(int from) throws IOException, SQLException {
		Initialize init = new Initialize();
//		 init.run("jdbc:mysql://192.168.3.107:3306/test", "user", "pwd",
//		 "select * from site_item_price", from);
		init.run("jdbc:mysql://192.168.1.30:3306/extension", "root",
				"4rfv&UJM", "select * from site_item_price", from);
	}

	public static void merge() throws IOException, SQLException {
		MergeWeight mer = new MergeWeight();
		mer.run();
	}
	
	public static void update() throws IOException, SQLException{
		Update update = new Update();
		update.run("jdbc:mysql://192.168.1.30:3306/extension", "root",
				"4rfv&UJM", "select * from site_item");
//		update.run("jdbc:mysql://192.168.3.107:3306/test", "user", "pwd",
//				 "select * from site_item");
	}

	public static void select(int K, String date) throws IOException,
			ParseException, SQLException {
		SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date dateTime = fmt.parse(date);
		System.out.println(dateTime);
		TopK select = new TopK();
		Item[] items = select.run(K, dateTime.getTime());
		for (Item item : items) {
			if (item == null)
				continue;
			System.out.println("itemId = " + item.id + ", weight = "
					+ item.weight + ", price = "+item.current_pice+", update_time = "
					+ fmt.format(item.update_time));
		}
	}
}
