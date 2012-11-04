package org.brackit.berkeleydb.binding.typebinding;

import java.util.Date;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class DateBinding extends TupleBinding<Date> {

	@Override
	public Date entryToObject(TupleInput input) {
		long date = input.readLong();
		return new Date(date);
	}

	@Override
	public void objectToEntry(Date date, TupleOutput output) {
		output.writeLong(date.getTime());
	}

}
