package org.sg.policy;

import java.io.IOException;
import java.sql.SQLException;

import org.sg.data.Item;
import org.sg.util.Heap;

public class TopK extends Operate {

	public Item[] run(int K, long baseTime) throws IOException, SQLException {
		openFiles();
		Heap heap = new Heap(K);
		Item item = null;
		metaFile.goHead((int) maxId);
		while ((item = metaFile.nextItem()) != null) {
			if (item.id % 100000 == 0) // 进度提示
				System.out.format("topN itemid = %d\n", item.id);
			if (item.elemLength == 0 || item.update_time < baseTime
					|| item.weight > 8 || item.current_pice < 0
					|| item.min_price != item.current_pice)
				continue;
			heap.add(item);
		}
		heap.sort();
		closeFiles();
		return heap.iterator();
	}
}
