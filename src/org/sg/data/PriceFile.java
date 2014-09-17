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
	private static final int priceBufferSize = elemCount * fileCount;

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

	/** 添加新价格，更新新权重 */
	public void updateItem(Item item, float newPrice, long newUpdateTime)
			throws IOException {
		updateWeight(item, newPrice); // 更新 weight
		++item.elemLength;
		item.current_pice = newPrice; // 更新 price
		if (newUpdateTime > item.update_time) {
			item.update_time = newUpdateTime;
			if (newPrice < item.min_price)
				item.min_price = newPrice; // 更新 min_price
		}
	}

	/** 通过历史价格更新权重 */
	public void updateItem(Item item) throws IOException {
		if (item.elemLength == 0)
			return;
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
		item.weight = sum / count / item.current_pice;
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
	
	private int count(int length) {
		return (length > priceBufferSize) ? priceBufferSize : length;
	}

	private void updateWeight(Item item, float price) throws IOException {
		int oldCount = count(item.elemLength);
		if (item.elemLength >= priceBufferSize) {
			float oldPrice = readPrice(item.id, item.elemLength + 1);
			item.weight = (item.weight * item.current_pice * oldCount + price - oldPrice)
					/ oldCount/price;
		} else
			item.weight = (item.weight * item.current_pice * oldCount + price)
					/ (oldCount + 1)/price;
	}

}
