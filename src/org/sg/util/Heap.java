package org.sg.util;

import org.sg.data.Item;

public class Heap {
	private Item[] heap; // 堆地址
	private int length; // 元素个数
	private Item temp; // 元素交换缓存

	/**
	 * 构造函数
	 * 
	 * @param int length 初始化堆的大小
	 */
	public Heap(int length) {
		heap = new Item[length];
		this.length = 0;
	}

	/** 清空堆 */
	public void clear() {
		this.length = 0;
	}

	/**
	 * 增加元素
	 * 
	 * @param Item
	 *            item 待增加的元素
	 * @return void
	 * */
	public void add(Item item, long baseTime) {
		if (item.update_time < baseTime || item.elemLength == 0
				|| item.current_pice <= 0 || item.weight <= 1
				|| item.weight > 10 || item.min_price != item.current_pice) // 业务筛选逻辑（可更改）
			return;
		if (length < heap.length) {
			heap[length++] = item;
			rebalance_tail();
		} else if (item.weight > heap[0].weight) {
			heap[0] = item;
			rebalance_head(length);
		}
	}

	/**
	 * 堆排序
	 * 
	 * @param void
	 * @return boolean 排序结果布尔值
	 * */
	public void sort() {
		while (length-- > 1) {
			swap(length, 0);
			rebalance_head(length);
		}
	}

	/* 返回堆结果 */
	public Item[] iterator() {
		return heap;
	}

	/* 尾部元素再平衡 */
	private void rebalance_tail() {
		int tail = length - 1;
		int parent = 0;
		while (tail > 0) {
			parent = parentId(tail);
			if (heap[parent].weight > heap[tail].weight) {
				swap(parent, tail);
				tail = parent;
			} else
				break;
		}
	}

	private int parentId(int id) {
		if (id < 1)
			return -1;
		return ((id & 1) == 1) ? id >> 1 : (id >> 1) - 1;
	}

	/* 首部元素再平衡 */
	private void rebalance_head(int length) {
		int start = 0;
		int lchild = 1;
		int rchild = 2;
		while (lchild < length) {
			if (rchild >= length) {
				if (heap[start].weight > heap[lchild].weight)
					swap(start, lchild);
				break;
			} else if (heap[start].weight <= heap[lchild].weight
					&& heap[start].weight <= heap[rchild].weight)
				break;
			else if (heap[lchild].weight < heap[rchild].weight) {
				swap(lchild, start);
				start = lchild;
			} else {
				swap(rchild, start);
				start = rchild;
			}
			lchild = (start << 1) + 1;
			rchild = (start << 1) + 2;
		}
	}

	/* 元素交换 */
	private void swap(int lindex, int rindex) {
		temp = heap[lindex];
		heap[lindex] = heap[rindex];
		heap[rindex] = temp;
	}

}
