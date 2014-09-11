package org.sg.data;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

public class PriceFile extends SFile {
	private static final int elemSize = 4; // (price)
	private static final int elemCount = 10; // 每个文件对同id记录的存储量
	private static final String dataFileName = "./lib/suggest.dt_"; // 文件名前缀
	public static final int priceBufferSize = elemCount * fileCount;

	/* file index */
	private int fileNum; // 文件编号缓存
	private RandomAccessFile dataFile; // 文件句柄缓存

	/* 写价格 */
	public void writePrice(int id, int elemLength, float price)
			throws IOException {
		long locate = getDataFile(id, elemLength);
		dataFile.seek(locate);
		dataFile.writeFloat(price);
	}

	/* 读价格 */
	public float readPrice(int id, int elemLength) throws IOException {
		long locate = getDataFile(id, elemLength - 1);
		if (locate < dataFile.length()) {
			dataFile.seek(locate);
			return dataFile.readFloat();
		}
		return -1;
	}

	/** 添加新价格，返回新权重 */
	public float addPrice(int id, Item item, float newPrice) throws IOException {
		writePrice(id, item.elemLength, newPrice); // 写入新价格
		int oldCount = (item.elemLength > priceBufferSize) ? priceBufferSize
				: item.elemLength;
		if (newPrice < 0)
			return item.weight;
		return (item.weight * item.current_pice * oldCount + newPrice)
				/ ((oldCount < priceBufferSize) ? oldCount + 1 : oldCount);
	}

	private float minPrice = 0;
	private float itemPrice = 0;

	public float readWeight(int id, int elemLength) throws IOException {
		int count = (elemLength > priceBufferSize) ? priceBufferSize
				: elemLength;
		float sum = 0;
		int num = 0;
		int index = 0;
		long locate = 0;
		minPrice = 999999999;
		while (index < count) {
			locate = getDataFile(id, index);
			dataFile.seek(locate);
			for (num = 0; num < elemCount && index < count; ++num, ++index) {
				itemPrice = dataFile.readFloat();
				if (itemPrice < 0)
					continue;
				if (itemPrice < minPrice)
					minPrice = itemPrice;
				sum += itemPrice;
			}
		}
		return (count == 0) ? 0 : sum / count / itemPrice;
	}

	public float minPrice() {
		return minPrice;
	}

	public float curPrice() {
		return itemPrice;
	}

	/* 获得存储文件 , 返回推荐文件偏移量 */
	private long getDataFile(int id, int elemLength)
			throws FileNotFoundException {
		updateItemid(id);
		fileNum = indexNum * fileCount + elemLength / elemCount % fileCount;
		if (fileNum >= files.length)
			files = Arrays.copyOf(files, 3 * fileNum / 2);
		if (files[fileNum] == null)
			files[fileNum] = new RandomAccessFile(new File(dataFileName
					+ fileNum), "rw");
		dataFile = files[fileNum];
		return (itemid * elemCount + elemLength % elemCount) * elemSize;
	}

}
