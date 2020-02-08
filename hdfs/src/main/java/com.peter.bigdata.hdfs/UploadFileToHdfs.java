package com.peter.bigdata.hdfs;

import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author chen_wj
 * @Description:
 * @date 2020/2/7
 * @Description:
 * @modifier
 */
public class UploadFileToHdfs {

	public static void main(String[] args) throws IOException, URISyntaxException {
//		uploadFile();
		uploadFiles();
	}

	/**
	 * upload local file to hdfs
	 */
	public static void uploadFile() throws IOException, URISyntaxException {
		FileSystem hdfs = FileSystemUtil.getHdfsFileSystem();
		Path src = new Path("/Users/peter/work/intellijSpace/bigdata/test_upload.txt");
		Path target = new Path("hdfs://cluster-100:9000/bigdata/hdfs");
		hdfs.copyFromLocalFile(src,target);
	}

	/**
	 * upload many files to hdfs
	 */
	public static void uploadFiles() throws IOException, URISyntaxException {
		FileSystem hdfs = FileSystemUtil.getHdfsFileSystem();
		FileSystem localFs = FileSystemUtil.getLocalFileSystem();
		PathFilter filter = new PathFilter() {
			@Override
			public boolean accept(Path path) {
				/**
				 * filter txt file
				 */
				return path.toString().lastIndexOf(".txt") != -1;
			}
		};
		FileStatus[] localFileStatusArr = localFs.globStatus(
				new Path("/Users/peter/work/intellijSpace/bigdata/testfolder/*"),filter);
		Path[] localPathArr = FileUtil.stat2Paths(localFileStatusArr);
		for(Path path : localPathArr) {
			hdfs.copyFromLocalFile(
					path,
					new Path("hdfs://cluster-100:9000/bigdata/hdfs/filter"));
		}
	}

}
