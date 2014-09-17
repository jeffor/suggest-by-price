package org.sg.data;

import java.io.IOException;
import java.io.RandomAccessFile;

public class SFile {
	protected static final int fileCount = 10;
	protected static final int capacity = 50000000; // 每个文件的元素容量

	protected int itemid; // 当前id缓存
	protected int indexNum; // 当前文件编号缓存
	protected RandomAccessFile[] files = new RandomAccessFile[fileCount]; // 文件列表

	/* 将一个id转换成 indexNum 和 itemid */
	protected void updateItemid(int id) {
		indexNum = id / capacity;
		itemid = id % capacity;
	}

	/* 关闭文件 */
	public void closeFiles() throws IOException {
		for (int index = 0; index < files.length; ++index) {
			if (files[index] != null) {
				files[index].close();
				files[index] = null;
			}
		}
	}
}
