package com.peter.bigdata.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author chen_wj
 * @Description:
 * @date 2020/2/6
 * @Description:
 * @modifier
 */
public class MakeFileOrDirectory {

	public static void main(String[] args) throws IOException, URISyntaxException {
		makeDirOrFile();
	}

	/**
	 *  make director or file
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static void makeDirOrFile() throws IOException, URISyntaxException {
		FileSystem hdfsFS = FileSystemUtil.getHdfsFileSystem();

		/**
		 * make directory "/bigdata/hdfs"
		 */
		hdfsFS.mkdirs(new Path("hdfs://cluster-100:9000/bigdata/hdfs"));

		/**
		 * make file "/bigdata/hdfs/test.txt"
		 */
		hdfsFS.createNewFile(new Path("hdfs://cluster-100:9000/bigdata/hdfs/test.txt"));
	}

}
