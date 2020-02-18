package com.peter.bigdata.hdfs;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author chen_wj
 * @Description:
 * @date 2020/2/7
 * @Description:
 * @modifier
 */
public class GetHdfsNodeInfo {

	public static void main(String[] args) throws IOException, URISyntaxException {
//		getHdfsNodes();
		getFileBlockLocation();
	}

	/**
	 * get DataNodeInfo
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static void getHdfsNodes() throws IOException, URISyntaxException {
		FileSystem hdfs = FileSystemUtil.getHdfsFileSystem();
		DatanodeInfo[] dataNodeInfos = ((DistributedFileSystem)hdfs).getDataNodeStats();
		for(int i = 0, len = dataNodeInfos.length; i < len; i++ ) {
			System.out.println("第" + i + "个DataNode:" + dataNodeInfos[i].getHostName());
			System.out.println("第" + i + "个DataNode:" + dataNodeInfos[i]);
		}
	}

	public static void getFileBlockLocation() throws IOException, URISyntaxException {
		FileSystem hdfs = FileSystemUtil.getHdfsFileSystem();
		Path path = new Path("hdfs://cluster-100:9000/bigdata/hdfs/test_upload.txt");
		FileStatus fileStatus = hdfs.getFileStatus(path);
		BlockLocation[] blockLocations = hdfs.getFileBlockLocations(path,0, fileStatus.getLen());
		System.out.println("===>" + blockLocations.length);
		for(int i = 0, len = blockLocations.length; i < len; i++) {
			System.out.println("block " + i + ":" + blockLocations[i]);
		}
	}

}
