package org.brackit.berkeleydb.tuple;

public class AtomicChar extends Atomic<Character> implements Comparable<AtomicChar> {

	private Character data;
	
	public AtomicChar(String fieldName, char data){
		super(fieldName);
		this.data = data;
	}
	
	@Override
	public Character getData() {
		return data;
	}
	
	@Override
	public String toString() {
		return data.toString();
	}

	public int compareTo(AtomicChar o) {
		return data.compareTo(o.getData());
	}

}
