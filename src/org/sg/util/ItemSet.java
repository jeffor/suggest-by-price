package org.sg.util;

import java.util.Arrays;

import org.sg.data.Item;

public class ItemSet {
	private Item[] set = new Item[10000];
	public int length;
	
	public ItemSet(){
		length = 0;
	}
	
	public void clear(){
		length = 0;
	}
	
	public void add(Item item, long baseTime){
		if (item.update_time < baseTime || item.elemLength == 0
				|| item.current_pice < 0 || item.weight <= 1
				|| item.weight > 10 || item.min_price != item.current_pice) // 业务筛选逻辑（可更改）
			return;
		if(length == set.length)
			set = Arrays.copyOf(set, 3*set.length/2);
		set[length++] = item;
	}
	
	public Item[] iterator(){
		return set;
	}

}
