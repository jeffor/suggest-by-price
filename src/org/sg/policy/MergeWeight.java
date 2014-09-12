package org.sg.policy;

import java.io.IOException;
import java.sql.SQLException;

import org.sg.data.Item;

public class MergeWeight extends Operate {

	public void run() throws IOException, SQLException {
		openFiles();
		Item item = null;
		metaFile.goHead((int) maxId);
		while ((item = metaFile.nextItem()) != null) {
			if (item.elemLength == 0)
				continue;
			priceFile.caculateWeightWitenUpdateItem(item);
			metaFile.write(item);
			if (item.id % 100000 == 0)
				System.out.println("merge id = " + item.id);
		}
		closeFiles();
	}

}
