package org.sg.data;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

public class MetaFile extends SFile {
	static private final String metaFileName = "./lib/suggest.idx_"; // 文件名前缀
	static protected final int metaSize = 24; // 元素大小
	/* meta data filed */
	private RandomAccessFile metaFile; // 当前文件句柄缓存
	private int maxId = 0;
	private int itrId = 0;

	/* 读取元数据 elemLength, fileNum */
	public Item read(int id) throws IOException {
		long locate = getMetaFile(id);
		if (locate < metaFile.length()) {
			metaFile.seek(locate);
			return new Item(id, metaFile.readInt(), metaFile.readFloat(),
					metaFile.readLong(), metaFile.readFloat(), metaFile.readFloat());
		} else {
			return new Item(id, 0, 0, 0, 0, 999999999);
		}
	}

	public void goHead(int maxId) throws IOException {
		itrId = 0;
		this.maxId = maxId;
	}

	public Item nextItem() throws IOException {
		if (itrId <= maxId) {
			if (itrId % capacity == 0)
				getMetaFile(itrId);
			if (metaFile.length() <= metaFile.getFilePointer())
				return new Item(itrId++, 0, 0, 0, 0, 999999999);
			return new Item(itrId++, metaFile.readInt(), metaFile.readFloat(),
					metaFile.readLong(), metaFile.readFloat(), metaFile.readFloat());
		}
		return null;
	}

	/* 写元数据文件 */
	public void write(int id, Item item)
			throws IOException {
		long locate = getMetaFile(id);
		metaFile.seek(locate);
		metaFile.writeInt(item.elemLength);
		metaFile.writeFloat(item.weight);
		metaFile.writeLong(item.update_time);
		metaFile.writeFloat(item.current_pice);
		metaFile.writeFloat(item.min_price);
	}

	/* 获得元数据文件，返回推荐指针偏移量 */
	private long getMetaFile(int id) throws IOException {
		updateItemid(id);
		if (indexNum >= files.length)
			files = Arrays.copyOf(files, 3 * indexNum / 2);
		if (files[indexNum] == null)
			files[indexNum] = new RandomAccessFile(new File(metaFileName
					+ indexNum), "rw");
		metaFile = files[indexNum];
		return itemid * metaSize;
	}

//	RandomAccessFile[] newFiles = new RandomAccessFile[fileCount];
//	RandomAccessFile newFile = null;
//
//	private void writeNewItem(int id, int elemLength, float weight, long date)
//			throws IOException {
//		long locate = getNewFile(id);
////		System.out.println("write new file "+id	);
//		newFile.seek(locate);
//		newFile.writeInt(elemLength);
//		newFile.writeFloat(weight);
//		newFile.writeLong(date);
//	}
//
//	private long getNewFile(int id) throws IOException {
//		if (indexNum >= newFiles.length)
//			newFiles = Arrays.copyOf(newFiles, 3 * indexNum / 2);
//		if (newFiles[indexNum] == null)
//			newFiles[indexNum] = new RandomAccessFile(new File("./lib/suggest.meta_"
//					+ indexNum), "rw");
//		newFile = newFiles[indexNum];
//		return (id%capacity) * (metaSize + 5);
//	}
}