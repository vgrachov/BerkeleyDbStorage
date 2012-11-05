package org.brackit.berkeleydb.tuple;

import java.util.Date;

import org.brackit.berkeleydb.binding.typebinding.DateBinding;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.TupleBinding;

public final class DatabaseBindingHelper {
	
	private DateBinding dateBinding = new DateBinding();

	private static class DatabaseBindingHelperHolder{
		private static DatabaseBindingHelper instance = new DatabaseBindingHelper(); 
	}
	
	private DatabaseBindingHelper(){
		
	}
	
	public static DatabaseBindingHelper getInstance(){
		return DatabaseBindingHelperHolder.instance;
	}
	
	public final EntryBinding databaseBinding(ColumnType columnType){
		if (columnType == ColumnType.String)
			return TupleBinding.getPrimitiveBinding(String.class);
		else
		if (columnType == ColumnType.Integer)
			return TupleBinding.getPrimitiveBinding(Integer.class);
		else
		if (columnType == ColumnType.Double)
			return TupleBinding.getPrimitiveBinding(Double.class);
		if (columnType == ColumnType.Char)
			return TupleBinding.getPrimitiveBinding(Character.class);
		else
		if (columnType == ColumnType.Date)
			return dateBinding;
		else
			throw new IllegalArgumentException("Column type has not corresponding binding");
	}

}
