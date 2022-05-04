package cs245.as3;

import java.nio.ByteBuffer;
import java.util.*;

import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import cs245.as3.interfaces.StorageManager.TaggedValue;

/**
 * You will implement this class.
 *
 * The implementation we have provided below performs atomic transactions but the changes are not durable.
 * Feel free to replace any of the data structures in your implementation, though the instructor solution includes
 * the same data structures (with additional fields) and uses the same strategy of buffering writes until commit.
 *
 * Your implementation need not be threadsafe, i.e. no methods of TransactionManager are ever called concurrently.
 *
 * You can assume that the constructor and initAndRecover() are both called before any of the other methods.
 */
public class TransactionManager {
	class WritesetEntry {
		public long key;
		public byte[] value;
		public WritesetEntry(long key, byte[] value) {
			this.key = key;
			this.value = value;
		}
	}
	/**
	  * Holds the latest value for each key.
	  */
	private HashMap<Long, TaggedValue> latestValues;
	/**
	  * Hold on to writesets until commit.
	  */
	private HashMap<Long, ArrayList<WritesetEntry>> writesets;
	public StorageManager sm;
	public LogManager lm;
	LinkedHashMap<Integer,HashSet<Long>> keytag;
	public TransactionManager() {
		writesets = new HashMap<>();
		keytag=new LinkedHashMap<>();
		//see initAndRecover
		latestValues = null;
	}
	/**
	 * Prepare the transaction manager to serve operations.
	 * At this time you should detect whether the StorageManager is inconsistent and recover it.
	 */
	public void initAndRecover(StorageManager sm, LogManager lm) {
		latestValues = sm.readStoredTable();
		this.sm=sm;
		this.lm=lm;
		Map<Long,Integer> TxnTag=new HashMap<>();
		int logCurOffset=lm.getLogTruncationOffset();
		int logEndOffset=lm.getLogEndOffset();
		while(logCurOffset<logEndOffset){
			ByteBuffer header=ByteBuffer.allocate(14);
			header.put(lm.readLogRecord(logCurOffset,14));
			logCurOffset+=14;
			char flag=header.getChar(0);
			long txnid=header.getLong(2);
			switch (flag){
				case 's':
					TxnTag.put(txnid, (logCurOffset-14));
					break;
				case 'r':
					int length=header.getInt(10);
					ByteBuffer txnrecord=ByteBuffer.allocate(length);
					txnrecord.put(lm.readLogRecord(logCurOffset,length));
					long key = txnrecord.getLong(0);
					byte[] entrytobyte = txnrecord.array();
					byte[] value = new byte[entrytobyte.length - 8];
					for (int i = 8, j = 0; i < entrytobyte.length; i++, j++) {
						value[j] = entrytobyte[i];
					}
					write(txnid,key,value);
					logCurOffset+=length;
					break;
				case 'e':
					ArrayList<WritesetEntry> writeset = writesets.get(txnid);
					//  write txn start entry
					long tag = TxnTag.get(txnid);
					if (writeset != null) {
						HashSet<Long> keyset=new HashSet<>();
						for(WritesetEntry x : writeset) {
							sm.queueWrite(x.key, tag, x.value);
							latestValues.put(x.key, new TaggedValue(0, x.value));
							keyset.add(x.key);
						}
						keytag.put((int)tag,keyset);
						writesets.remove(txnid);
					}
					break;
				default:
					throw new RuntimeException("log analysis error");
			}
		}

	}

	/**
	 * Indicates the start of a new transaction. We will guarantee that txID always increases (even across crashes)
	 */
	public void start(long txID) {
		// TODO: Not implemented for non-durable transactions, you should implement this
	}

	/**
	 * Returns the latest committed value for a key by any transaction.
	 */
	public byte[] read(long txID, long key) {
		TaggedValue taggedValue = latestValues.get(key);
		return taggedValue == null ? null : taggedValue.value;
	}

	/**
	 * Indicates a write to the database. Note that such writes should not be visible to read() 
	 * calls until the transaction making the write commits. For simplicity, we will not make reads 
	 * to this same key from txID itself after we make a write to the key. 
	 */
	public void write(long txID, long key, byte[] value) {
		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		if (writeset == null) {
			writeset = new ArrayList<>();
			writesets.put(txID, writeset);
		}
		writeset.add(new WritesetEntry(key, value));
	}
	/**
	 * Commits a transaction, and makes its writes visible to subsequent read operations.\
	 */
	public void commit(long txID) {
		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		//  write txn start entry
		if (writeset != null) {
			for(WritesetEntry x : writeset) {
				latestValues.put(x.key, new TaggedValue(0, x.value));
			}
			ByteBuffer txnstartflag=ByteBuffer.allocate(14);
			txnstartflag.putChar('s'); //s means txn start
			txnstartflag.putLong(txID);
			txnstartflag.putInt(0); //Add redundancy to align the headers of the log entry
			int tag=lm.appendLogRecord(txnstartflag.array());
			for(WritesetEntry x : writeset) {
				// write log entry
				writeRedoLog(txID, x);
			}
			// write txn end entry
			ByteBuffer txnendflag=ByteBuffer.allocate(14);
			txnendflag.putChar('e'); //e means txn end
			txnendflag.putLong(txID);
			txnendflag.putInt(0); //Add redundancy to align the headers of the log entry
			lm.appendLogRecord(txnendflag.array());

			HashSet<Long> keyset=new HashSet<>();
			for(WritesetEntry x : writeset) {
				sm.queueWrite(x.key, tag, x.value);
				keyset.add(x.key);
			}
			keytag.put(tag,keyset);
		}
		writesets.remove(txID);
	}
	/**
	 * Aborts a transaction.
	 */
	public void abort(long txID) {
		writesets.remove(txID);

	}

	/**
	 * The storage manager will call back into this procedure every time a queued write becomes persistent.
	 * These calls are in order of writes to a key and will occur once for every such queued write, unless a crash occurs.
	 */
	public void writePersisted(long key, long persisted_tag, byte[] persisted_value) {
		if(!keytag.containsKey((int)persisted_tag)){
			return;
		}
		HashSet<Long> keyset=keytag.get((int)persisted_tag);

		if(keyset.contains(key)) {
			keyset.remove(key);
		}
		if (keyset.size()==0){
			keytag.remove((int)persisted_tag);
		}
		if (keytag.size()!=0){
			Map.Entry<Integer,HashSet<Long>> fitst=keytag.entrySet().iterator().next();
			long tag=fitst.getKey();
			lm.setLogTruncationOffset((int)tag);
			return;
		}else {
			lm.setLogTruncationOffset(lm.getLogEndOffset());
		}
	}
	public void writeRedoLog(long txID,WritesetEntry x){
		ByteBuffer logbyte=ByteBuffer.allocate(22+x.value.length);
		logbyte.putChar('r');
		logbyte.putLong(txID);
		logbyte.putInt(8+x.value.length);
		logbyte.putLong(x.key);
		logbyte.put(x.value);
		lm.appendLogRecord(logbyte.array());
	}

}
