package com.peter.bigdata.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author chen_wj
 * @Description:
 * @date 2020/2/7
 * @Description:
 * @modifier
 */
public class ListFiles {

	public static void main(String[] args) throws IOException, URISyntaxException {
		listFiles();
	}

	public static void listFiles() throws IOException, URISyntaxException {
		FileSystem hdfs = FileSystemUtil.getHdfsFileSystem();
		FileStatus[] fileStatusArr = hdfs.globStatus(
				new Path("hdfs://cluster-100:9000/bigdata/hdfs/filter/*"));
		Path[] pathArr = FileUtil.stat2Paths(fileStatusArr);
		for(FileStatus fileStatus : fileStatusArr) {
			System.out.println(fileStatus);
		}
		for(Path path : pathArr) {
			System.out.println(path);
		}
	}
}
