package jp.co.rakuten.rit.roma.tools.tc;

import java.util.Iterator;
import java.util.Properties;

import tokyocabinet.HDB;

public class TCHashDataStoreUtil {

	public static class Entry {
		public byte[] key;

		public byte[] vnode; // 32bits

		public byte[] pclock; // 32bits

		public byte[] lclock; // 32bits

		public byte[] expiry; // 32bits
		
		//public int flag;

		public byte[] value;

		// [vn, Time.now.to_i, clk, expt, v]
		public Entry(byte[] key, byte[] vnode, byte[] pclock, byte[] lclock,
				byte[] expiry, byte[] value) {
			this.key = key;
			this.vnode = vnode;
			this.pclock = pclock;
			this.lclock = lclock;
			this.expiry = expiry;
			this.value = value;
		}
	}

	private HDB tchdb;

	private long iterationSleepTime;

	private int iterationSleepCount;

	public TCHashDataStoreUtil() {
		tchdb = null;
	}

	public void init(final Properties props) {
		tchdb = new HDB();
		String path = props.getProperty(Config.FILE_PATH_NAME,
				Config.DEFAULT_FILE_PATH_NAME);
		iterationSleepTime = Long.parseLong(props.getProperty(
				Config.SLEEP_TIME_OF_ITERATION,
				Config.DEFAULT_SLEEP_TIME_OF_ITERATION));
		iterationSleepCount = Integer.parseInt(props.getProperty(
				Config.SLEEP_COUNT_OF_ITERATION,
				Config.DEFAULT_SLEEP_COUNT_OF_ITERATION));
		if (!tchdb.open(path, HDB.OREADER | HDB.ONOLCK | HDB.OWRITER)) {
			throw new RuntimeException(tchdb.errmsg());
		}
	}

	public Iterator<Entry> iterator() {
		if (!tchdb.iterinit()) {
			throw new RuntimeException(tchdb.errmsg());
		}
		return new Iterator<Entry>() {
			private byte[] key = null;

			private int count = 0;

			@Override
			public boolean hasNext() {
				key = tchdb.iternext();
				return key != null;
			}

			@Override
			public Entry next() {
				if (count > iterationSleepCount) {
					count = 0;
					try {
						Thread.sleep(iterationSleepTime);
					} catch (InterruptedException e) {
					}
				}
				count++;
				return getEntry(key);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
	
	public void putEntry(Entry e) {
		if (e == null) {
			throw new NullPointerException("entry is null");
		}
		int len = 16 + e.value.length;
		byte[] rawData = new byte[len];
		// [ 0.. 3] vn
		System.arraycopy(e.vnode, 0, rawData, 0, 4);
		// [ 4.. 7] physical clock (unix time)
		System.arraycopy(e.pclock, 0, rawData, 4, 4);
		// [ 8..11] logical clock
		System.arraycopy(e.lclock, 0, rawData, 8, 4);
		// [12..15] exptime(unix time)
		System.arraycopy(e.expiry, 0, rawData, 12, 4);
		// [16.. ] value data
		System.arraycopy(e.value, 0, rawData, 16, e.value.length);
		if(!tchdb.put(e.key, rawData)) {
			throw new RuntimeException("storing fail: " + e.key);
		}
	}

	public Entry getEntry(byte[] key) {
		byte[] rawData = tchdb.get(key);
		if (rawData == null) {
			throw new RuntimeException("no record: " + (new String(key)));
		}

		// [ 0.. 3] vn
		byte[] vnode = new byte[4];
		System.arraycopy(rawData, 0, vnode, 0, 4);
		// [ 4.. 7] physical clock (unix time)
		byte[] pclock = new byte[4];
		System.arraycopy(rawData, 4, pclock, 0, 4);
		// [ 8..11] logical clock
		byte[] lclock = new byte[4];
		System.arraycopy(rawData, 8, lclock, 0, 4);
		// [12..15] exptime(unix time)
		byte[] expiry = new byte[4];
		System.arraycopy(rawData, 12, expiry, 0, 4);
		// [16.. ] value data
		byte[] value = new byte[rawData.length - 16];
		System.arraycopy(rawData, 16, value, 0, value.length);
		Entry e = new Entry(key, vnode, pclock, lclock, expiry, value);
		return e;
	}

	public static void main(String[] args) throws Exception {
		TCHashDataStoreUtil util = new TCHashDataStoreUtil();
		Properties props = new Properties();
		props.setProperty(Config.FILE_PATH_NAME, "./6.tc");
		props.setProperty(Config.SLEEP_TIME_OF_ITERATION, "100");
		props.setProperty(Config.SLEEP_COUNT_OF_ITERATION, "100");
		util.init(props);
		Iterator<Entry> iter = util.iterator();
		while (iter.hasNext()) {
			Entry e = iter.next();
			System.out.println("key: " + (new String(e.key)));
			System.out.println("val: " + (new String(e.value)));
		}
	}
}
