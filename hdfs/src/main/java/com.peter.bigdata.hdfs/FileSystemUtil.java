package com.peter.bigdata.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author chen_wj
 * @Description:
 * @date 2020/2/6
 * @Description: File System Util
 * @modifier
 */
public class FileSystemUtil {

	private static FileSystem hdfsFileSystem;

	private static FileSystem localFileSystem;

	public static void main(String[] args) throws IOException, URISyntaxException {
		getHdfsFileSystem();
		getLocalFileSystem();
	}

	private FileSystemUtil() {

	}

	/**
	 * get hdfs fileSystem by uri
	 * @return FileSystem
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	public static synchronized FileSystem getHdfsFileSystem() throws URISyntaxException, IOException {
		if(hdfsFileSystem == null) {
			Configuration conf = new Configuration();
			URI uri = new URI("hdfs://cluster-100:9000");
			hdfsFileSystem = FileSystem.get(uri,conf);
			System.out.println(hdfsFileSystem);
		}
		return hdfsFileSystem;
	}


	/**
	 * get Local FileSystem
	 * @return
	 * @throws IOException
	 */
	public static synchronized FileSystem getLocalFileSystem() throws IOException {
		if(localFileSystem == null) {
			Configuration conf = new Configuration();
			localFileSystem = FileSystem.getLocal(conf);
			System.out.println(localFileSystem);
		}
		return localFileSystem;
	}

}
