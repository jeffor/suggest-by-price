package org.sg.data;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Date;

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

	private int count(int length) {
		return (length > priceBufferSize) ? priceBufferSize : length;
	}

	private int plusLength(int length) {
		return (length > priceBufferSize) ? (length % priceBufferSize) + 1
				+ priceBufferSize : length + 1;
	}

	private float updateWeight(Item item, float price, int count) {
		return (item.elemLength > priceBufferSize) ? (item.weight
				* item.current_pice * count + price - item.current_pice)
				/ count(item.elemLength) : (item.weight * item.current_pice
				* count + price)
				/ count(item.elemLength);
	}

	/** 添加新价格，返回新权重 */
	public void addPriceWithUpdateItem(Item item, float newPrice,
			Date newUpdateTime) throws IOException {
		writePrice(item.id, item.elemLength, newPrice); // 写入新价格
		int oldCount = count(item.elemLength);
		item.elemLength = plusLength(item.elemLength);
		item.weight = updateWeight(item, newPrice, oldCount);
		if (newUpdateTime != null && newUpdateTime.getTime() > item.update_time)
			item.update_time = newUpdateTime.getTime();
		item.current_pice = newPrice;
		if (newPrice < item.min_price)
			item.min_price = newPrice;
	}

	public void caculateWeightWitenUpdateItem(Item item) throws IOException {
		int count = count(item.elemLength);
		float sum = 0;
		long locate = 0;
		int index = 0;
		int num = 0;
		item.min_price = 999999999;
		while (index < count) {
			locate = getDataFile(item.id, index);
			dataFile.seek(locate);
			for (num = 0; num < elemCount && index < count; ++num, ++index) {
				item.current_pice = dataFile.readFloat();
				if (item.current_pice > 0 && item.current_pice < item.min_price)
					item.min_price = item.current_pice;
				sum += item.current_pice;
			}
		}
		item.weight = (count == 0) ? 0 : sum / count / item.current_pice;
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
