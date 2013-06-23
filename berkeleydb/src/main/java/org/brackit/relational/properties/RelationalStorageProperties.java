/*******************************************************************************
 * [New BSD License]
 *   Copyright (c) 2012-2013, Volodymyr Grachov <vladimir.grachov@gmail.com>  
 *   All rights reserved.
 *   
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions are met:
 *       * Redistributions of source code must retain the above copyright
 *         notice, this list of conditions and the following disclaimer.
 *       * Redistributions in binary form must reproduce the above copyright
 *         notice, this list of conditions and the following disclaimer in the
 *         documentation and/or other materials provided with the distribution.
 *       * Neither the name of the Brackit Project Team nor the
 *         names of its contributors may be used to endorse or promote products
 *         derived from this software without specific prior written permission.
 *   
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 *   ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *   WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *   DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 *   ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *   (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *   LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *   ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package org.brackit.relational.properties;

/**
 * This class represent all main properties confugured within project.
 * @author Volodymyr Grachov
 *
 */
public class RelationalStorageProperties {

	// set storage path as JVM argument
	private static final String storagePath = "/Users/vgrachov/Desktop/db_storage/100mb_mapper/";
	//private static final String storagePath = "/Users/vgrachov/Desktop/db_storage/100mb_mapper/";

	// set path to TBL files as JVM argument
	private static final String tblPath = "/Users/vgrachov/Desktop/db_storage/100mb/";

	public enum StorageEngine{
		BerkeleyDB,
		LevelDB
	}
	
	public enum MappingMode {
		DefaultTupleInputOutput,
		BStoreMapper
	}
	
	private RelationalStorageProperties() {
		
	}
	
	public static String getStoragePath(){
		return storagePath;
	}
	
	public static boolean inMemory(){
		return false;
	}
	
	public static boolean isTest(){
		return true;
	}
	
	public static StorageEngine getStorageEngine(){
		return StorageEngine.BerkeleyDB;
	}
	
	public static String getDatePattern(){
		return "yyyy-MM-dd";
	}
	
	public static String getTBLPath(){
		return tblPath; 
	}
	
	public static Integer getPageSize() {
		return 65536;
	}
	
	public static MappingMode getMappingMode() {
		return MappingMode.BStoreMapper;
	}
}
