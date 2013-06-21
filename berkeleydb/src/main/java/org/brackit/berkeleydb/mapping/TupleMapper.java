package org.brackit.berkeleydb.mapping;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import org.brackit.berkeleydb.binding.RelationalTupleBinding;
import org.brackit.berkeleydb.cursor.FullTableScanCursor;
import org.brackit.relational.metadata.Schema;
import org.brackit.relational.metadata.tuple.AtomicChar;
import org.brackit.relational.metadata.tuple.AtomicDate;
import org.brackit.relational.metadata.tuple.AtomicDouble;
import org.brackit.relational.metadata.tuple.AtomicInteger;
import org.brackit.relational.metadata.tuple.AtomicString;
import org.brackit.relational.metadata.tuple.AtomicValue;
import org.brackit.relational.metadata.tuple.Column;
import org.brackit.relational.metadata.tuple.ColumnType;
import org.brackit.relational.metadata.tuple.Tuple;
import org.brackit.relational.properties.RelationalStorageProperties;

import com.google.common.base.Preconditions;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/**
 * Implementation of mapper between {@link Tuple} and key-value pairs of BerkleyBD.
 * Works magnitude faster as native {@link TupleOutput} and {@link TupleInput}}
 * 
 */
public class TupleMapper {

	private final Schema schema;
	
	/**
	 * Precomputed position of field in key or value byte arrays.
	 */
	private static class FieldDescriptor {
		int reference;
		boolean isKey;
		ColumnType columnType;
		
		FieldDescriptor(int reference, boolean isKey, ColumnType columnType) {
			this.reference = reference;
			this.isKey = isKey;
			this.columnType = columnType;
		}
	}
	
	/**
	 * Holder for passing key-value pair to {@link Scan operators}
	 */
	public class KeyValuePair {
		
		private byte[] key,value;
		
		private KeyValuePair(byte[] key, byte[] value){
			this.key = key;
			this.value = value;
		}
		
		public byte[] getKey() {
			return key;
		}
		
		public byte[] getValue() {
			return value;
		}
	}
	
	private HashMap<String,FieldDescriptor> positionMap = new HashMap<String, FieldDescriptor>();  
	
	public TupleMapper(Schema schema) {
		this.schema = schema;
		positionPrecomputation();
	}
	
	/*
	 * return size value of entry in fixed-length part of key or value. doesn't correspond 1-1 to size of types in Java.
	 * String requires 4 bytes for reference and for length
	 */
	private int sizeOf(ColumnType columnType) {
		if (columnType == ColumnType.Char) return 2; else
		if (columnType == ColumnType.Date) return 8; else
		if (columnType == ColumnType.Double) return 8; else
		if (columnType == ColumnType.Integer) return 4; else
		if (columnType == ColumnType.String) return 4; else 
			throw new IllegalArgumentException("Size for type "+columnType + " is't defined");
	}
	
	/**
	 * compute position of each field in key and value byte arrays.
	 */
	private void positionPrecomputation() {
		int posKeyCounter = 0;
		int posValueCounter = 0;
		for (int i=0; i<schema.getColumns().length; i++) {
			Column column = schema.getColumns()[i];
			if (column.isBelongToKey()) {
				positionMap.put(column.getColumnName(), new FieldDescriptor(posKeyCounter, true, column.getType()));
				posKeyCounter += sizeOf(column.getType());
			} else {
				positionMap.put(column.getColumnName(), new FieldDescriptor(posValueCounter,false, column.getType()));
				posValueCounter += sizeOf(column.getType());
			}
		}
	}
	
	/**
	 * set byte representation of field value in key or value array.
	 */
	private void setByteReresentationToKeyValue(boolean isKey, byte[] buffer, byte[] key, byte[] value, int reference, int size) {
		if (isKey){
			System.arraycopy(buffer, 0, key, reference, size);
		} else {
			System.arraycopy(buffer, 0, value, reference, size);
		}
	}

	public KeyValuePair mapTupleToKeyValue(Tuple tuple) {
		// TODO(vgrachov): remove precondition checking. 
		Preconditions.checkNotNull(tuple);
		int keySize = 0;
		int valueSize = 0;
		int strLengthInKey = 0;
		int strLengthInValue = 0;
		for (int i=0; i<schema.getColumns().length; i++) {
			Column column = schema.getColumns()[i];
			int size = sizeOf(column.getType());
			if (schema.getColumns()[i].isBelongToKey())
				keySize += size;
			else
				valueSize +=size;
			if (column.getType() == ColumnType.String) {
				if (schema.getColumns()[i].isBelongToKey())
					strLengthInKey += ((AtomicString)tuple.getFields()[i]).getData().length();
				else
					strLengthInValue += ((AtomicString)tuple.getFields()[i]).getData().length();
			}
		}
		byte[] key = new byte[keySize + strLengthInKey];
		byte[] value = new byte[valueSize + strLengthInValue];
		
		// position of string blocks at the end of key and value byte arrays.
		int indexStrPositionKey = keySize;  
		int indexStrPositionValue = valueSize;
		
		for (int i=0; i<schema.getColumns().length; i++) {
			Column column = schema.getColumns()[i];
			FieldDescriptor fieldDescriptor = positionMap.get(column.getColumnName());
			if (column.getType() == ColumnType.Char) {
				char character = ((AtomicChar)tuple.getFields()[i]).getData();
				ByteBuffer buffer = ByteBuffer.allocate(sizeOf(column.getType()));
				buffer.putChar(character);
				setByteReresentationToKeyValue(column.isBelongToKey(), buffer.array(), key, value, fieldDescriptor.reference, sizeOf(column.getType()));
			} else
			if (column.getType() == ColumnType.Date) {
				long date = ((AtomicDate)tuple.getFields()[i]).getData();
				ByteBuffer buffer = ByteBuffer.allocate(sizeOf(column.getType()));
				buffer.putLong(date);
				setByteReresentationToKeyValue(column.isBelongToKey(), buffer.array(), key, value, fieldDescriptor.reference, sizeOf(column.getType()));
			} else
			if (column.getType() == ColumnType.Double) {
				double doubleValue = ((AtomicDouble)tuple.getFields()[i]).getData();
				ByteBuffer buffer = ByteBuffer.allocate(sizeOf(column.getType()));
				buffer.putDouble(doubleValue);
				setByteReresentationToKeyValue(column.isBelongToKey(), buffer.array(), key, value, fieldDescriptor.reference, sizeOf(column.getType()));
			} else
			if (column.getType() == ColumnType.Integer) {
				int intValue = ((AtomicInteger)tuple.getFields()[i]).getData();
				ByteBuffer buffer = ByteBuffer.allocate(sizeOf(column.getType()));
				buffer.putInt(intValue);
				setByteReresentationToKeyValue(column.isBelongToKey(), buffer.array(), key, value, fieldDescriptor.reference, sizeOf(column.getType()));
			} else
			if (column.getType() == ColumnType.String) {
				String stringValue = ((AtomicString)tuple.getFields()[i]).getData();
				byte[] strBytes = stringValue.getBytes();
				ByteBuffer stringRepresentationBlock = ByteBuffer.allocate(sizeOf(ColumnType.String));
				if (column.isBelongToKey()){
					stringRepresentationBlock.putShort((short)indexStrPositionValue);
					stringRepresentationBlock.putShort((short)strBytes.length);
					System.arraycopy(stringRepresentationBlock.array(), 0, key, fieldDescriptor.reference, sizeOf(column.getType()));
					System.arraycopy(strBytes, 0, key, indexStrPositionKey, strBytes.length);
					indexStrPositionKey += strBytes.length;
				} else {
					stringRepresentationBlock.putShort((short)indexStrPositionValue);
					stringRepresentationBlock.putShort((short)strBytes.length);
					System.arraycopy(stringRepresentationBlock.array(), 0, value, fieldDescriptor.reference, sizeOf(column.getType()));
					System.arraycopy(strBytes, 0, value, indexStrPositionValue, strBytes.length);
					indexStrPositionValue += strBytes.length;
				}
			}				
		}
		return new KeyValuePair(key, value);
	}
	
	public AtomicValue getValueByFieldName(String fieldName, byte[] key, byte[] value) {
		
		//TODO(vgrachov): Replace native hashmap with CERN Colt
		FieldDescriptor fieldDescriptor = positionMap.get(fieldName);
		ByteBuffer buffer = null;
		if (fieldDescriptor.isKey) {
			buffer = ByteBuffer.wrap(key, fieldDescriptor.reference, sizeOf(fieldDescriptor.columnType));
		} else {
			buffer = ByteBuffer.wrap(value, fieldDescriptor.reference, sizeOf(fieldDescriptor.columnType));
		}
		if (fieldDescriptor.columnType == ColumnType.Char)
			return new AtomicChar(fieldName, buffer.getChar());
		else
		if (fieldDescriptor.columnType == ColumnType.Date)
			return new AtomicDate(fieldName, buffer.getLong());
		else
		if (fieldDescriptor.columnType == ColumnType.Double)
			return new AtomicDouble(fieldName, buffer.getDouble());
		else
		if (fieldDescriptor.columnType == ColumnType.Integer)
			return new AtomicInteger(fieldName, buffer.getInt());
		else
		if (fieldDescriptor.columnType == ColumnType.String) {
			int stringPosition = buffer.getShort();
			int length = buffer.getShort();
			byte[] fieldValueBytes = new byte[length];
			if (fieldDescriptor.isKey) {
				System.arraycopy(key, stringPosition, fieldValueBytes, 0, length);
			} else {
				System.arraycopy(value, stringPosition, fieldValueBytes, 0, length);
			}
			return new AtomicString(fieldName, new String(fieldValueBytes));
		}
		throw new IllegalArgumentException(fieldName+" field can't be found");
	}
	
	public static void main(String[] args) {
		Column[] columns = new Column[]{
				new Column("lineitem", "l_orderkey", ColumnType.Integer, true, true),
				new Column("lineitem", "l_partkey", ColumnType.Integer, false, true),
				new Column("lineitem", "l_suppkey", ColumnType.Integer, false, true),
				new Column("lineitem", "l_linenumber", ColumnType.Integer, true, true),
				new Column("lineitem", "l_quantity", ColumnType.Double, false, false),
				new Column("lineitem", "l_extendedprice", ColumnType.Double, false, false),
				new Column("lineitem", "l_discount", ColumnType.Double, false, true),
				new Column("lineitem", "l_tax", ColumnType.Double, false, false),
				new Column("lineitem", "l_returnflag", ColumnType.Char, false, true),
				new Column("lineitem", "l_linestatus", ColumnType.Char, false, false),
				new Column("lineitem", "l_shipdate", ColumnType.Date, false, true),
				new Column("lineitem", "l_commitdate", ColumnType.Date, false, false),
				new Column("lineitem", "l_receiptdate", ColumnType.Date, false, true),
				new Column("lineitem", "l_shipinstruct", ColumnType.String, false, false),
				new Column("lineitem", "l_shipmode", ColumnType.String, false, true),
				new Column("lineitem", "l_comment", ColumnType.String, false, false)
		};
		Schema schema = new Schema(columns, "lineitem");
		SimpleDateFormat dateFormat = new SimpleDateFormat(RelationalStorageProperties.getDatePattern());
		String[] entries = "1|155190|7706|1|17|21168.23|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the|".split("\\|");
		AtomicValue[] fields = new AtomicValue[16];
		fields[0] = new AtomicInteger("l_orderkey", Integer.valueOf(entries[0]));
		fields[1] = new AtomicInteger("l_partkey", Integer.valueOf(entries[1]));
		fields[2] = new AtomicInteger("l_suppkey", Integer.valueOf(entries[2]));
		fields[3] = new AtomicInteger("l_linenumber", Integer.valueOf(entries[3]));
		fields[4] = new AtomicDouble("l_quantity", Double.valueOf(entries[4]));
		fields[5] = new AtomicDouble("l_extendedprice", Double.valueOf(entries[5]));
		fields[6] = new AtomicDouble("l_discount", Double.valueOf(entries[6]));
		fields[7] = new AtomicDouble("l_tax", Double.valueOf(entries[7]));
		fields[8] = new AtomicChar("l_returnflag", entries[8].charAt(0));
		fields[9] = new AtomicChar("l_linestatus", entries[9].charAt(0));
		try{
			fields[10] = new AtomicDate("l_shipdate", dateFormat.parse(entries[10]).getTime());
			fields[11] = new AtomicDate("l_commitdate", dateFormat.parse(entries[11]).getTime());
			fields[12] = new AtomicDate("l_receiptdate", dateFormat.parse(entries[12]).getTime());
		}catch (Exception e) {
			System.out.println(e.getMessage());
		}
		fields[13] = new AtomicString("l_shipinstruct", entries[13]);
		fields[14] = new AtomicString("l_shipmode", entries[14]);
		fields[15] = new AtomicString("l_comment", entries[15]);
		Tuple tuple = new Tuple(fields);
		TupleMapper fieldValuesMapper = new TupleMapper(schema);
		KeyValuePair kvPair = fieldValuesMapper.mapTupleToKeyValue(tuple);
		long start = System.currentTimeMillis();
/*		for (int i=0;i<1000000;i++) {
			fieldValuesMapper.getValueByFieldName("l_linestatus", kvPair.key, kvPair.value);
			AtomicValue restore = fieldValuesMapper.getValueByFieldName("l_shipdate", kvPair.key, kvPair.value);
			fieldValuesMapper.getValueByFieldName("l_shipdate", kvPair.key, kvPair.value);
		}*/
		for (int i=0; i<schema.getColumns().length; i++) {
			System.out.println(fieldValuesMapper.getValueByFieldName(schema.getColumns()[i].getColumnName(), kvPair.key, kvPair.value));
		}
		System.out.println(System.currentTimeMillis() - start);
	}

}
