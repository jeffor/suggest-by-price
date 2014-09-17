package org.sg.policy;

import java.io.IOException;
import java.sql.SQLException;

import org.sg.data.Item;
import org.sg.util.Heap;
import org.sg.util.ItemSet;

public class TopK extends Operate {
	private Heap heap;
	ItemSet set;
	
	public void run(int K, long baseTime) throws IOException, SQLException {
		openFiles();
		heap = new Heap(K);
		Item item = null;
		metaFile.goHead((int) maxItemid, 0);
		while ((item = metaFile.nextItem()) != null) {
			if (item.id % 100000 == 0) // 进度提示
				System.out.format("topN itemid = %d\n", item.id);
			heap.add(item, baseTime);
		}
		closeFiles();
	}
	
	public void run(long baseTime) throws IOException, SQLException {
		openFiles();
		set = new ItemSet();
		Item item = null;
		metaFile.goHead((int) maxItemid, 0);
		while ((item = metaFile.nextItem()) != null) {
			if (item.id % 100000 == 0) // 进度提示
				System.out.format("topN itemid = %d\n", item.id);
			set.add(item, baseTime);
		}
		closeFiles();
	}
	
	public Heap getHeap(){
		return heap;
	}
	
	public ItemSet getSet(){
		return set;
	}
	
	
}
