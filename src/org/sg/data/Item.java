package org.sg.data;

public class Item {
	public int id;
	public float weight;
	public int elemLength;
	public long update_time;
	public float current_pice;
	public float min_price;

	public Item(int id, int elemLength, float weight, long update_time,
			float current_pice, float minPrice) {
		this.id = id;
		this.elemLength = elemLength;
		this.weight = weight;
		this.update_time = update_time;
		this.current_pice = current_pice;
		min_price = minPrice;
	}
}