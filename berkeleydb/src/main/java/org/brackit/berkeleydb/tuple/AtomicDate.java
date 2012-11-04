package org.brackit.berkeleydb.tuple;

import java.util.Date;

public class AtomicDate extends Atomic<Date> implements Comparable<AtomicDate> {

	private Date data;

	public AtomicDate(String fieldName, long data){
		super(fieldName);
		this.data = new Date(data);
	}
	
	public AtomicDate(String fieldName, Date data){
		super(fieldName);
		this.data = data;
	}

	@Override
	public Date getData() {
		return data;
	}
	
	@Override
	public String toString() {
		return data.toString();
	}

	public int compareTo(AtomicDate o) {
		return data.compareTo(o.getData());
	}

}
