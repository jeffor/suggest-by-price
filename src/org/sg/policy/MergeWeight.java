package org.sg.policy;

import java.io.IOException;
import java.sql.SQLException;

import org.sg.data.Item;

public class MergeWeight extends Operate {

	public void run() throws IOException, SQLException {
		openFiles();
		Item item = null;
		for (int id = 0; id < maxId; ++id) {
			item = metaFile.read(id);
			if(id % 100000 == 0)
				System.out.println("merge id = "+id);
			if (item.elemLength == 0)
				continue;
			item.weight = priceFile.readWeight(id, item.elemLength);
			item.current_pice = priceFile.curPrice();
			item.min_price = priceFile.minPrice();
			metaFile.write(id, item);
		}
		closeFiles();
	}

}
