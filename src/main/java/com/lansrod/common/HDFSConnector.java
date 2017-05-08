package com.lansrod.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/**
 * HDFS Files access connector
 * 
 * @author fahd-externe.essid@edf.fr
 */
public class HDFSConnector  {

	//-----------------------------------------------------------------
	// CONSTRUCTOR
	//-----------------------------------------------------------------

	/**
	 * Creating HDFS connector using configuration xml files
	 * and kerberos user and keytab
	 */
	public HDFSConnector(String coreSiteFile, String hdfsSiteFile) {
		this.coreSiteFile = coreSiteFile;
		this.baseSiteFile = hdfsSiteFile;
	}

	//-----------------------------------------------------------------
	// IMPLEMENTATION
	//-----------------------------------------------------------------

	/**
	 * Configure hdfs connector from running instance xml properties files 
	 */
	private void configure() throws IOException {    
		conf = new Configuration();

		conf.addResource(coreSiteFile);
		conf.addResource(baseSiteFile);
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
	}

	/**
	 * HDFS Connection
	 */
	public void connect() throws IOException {
		configure();
		this.fs = FileSystem.get(conf);
	}

	/**
	 * HDFS Deconnection
	 */
	public void disconnect() throws IOException {
		fs.close();
	}

	public void cleanEmptyFiles(String dir) throws IOException {
		Path path = new Path(dir);

		if (path != null) {
			if (fs.exists(path)) {
				RemoteIterator<LocatedFileStatus> fileStatusIterator = fs.listFiles(path, true);
				while (fileStatusIterator.hasNext()) {
					LocatedFileStatus fileStatus = fileStatusIterator.next();

					if (fileStatus.getLen() == 0) {
						deleteFile(fileStatus.getPath());
					}

				}
			} 
		}
	}

	/**
	 * Create a hdfs directory
	 * @param path
	 */
	public void createDirectory(String path) throws IOException {
		fs.mkdirs(new Path(path));
	}

	/**
	 * Copy a file from a local directory to hdfs
	 * @param localFile
	 * @param target
	 */
	public void copyFromLocal(String localFile, String targetFile) throws IOException {
		Path local = new Path(localFile);
		Path target = new Path(targetFile);

		fs.copyFromLocalFile(local, target);
	}

	/**
	 * Copy a file from to local directory from HDFS
	 * @param localFile
	 * @param target
	 */
	public void copyToLocal(String distantFile, String localFile, boolean deleteSrc) throws IOException {
		Path distant = new Path(distantFile);
		Path local = new Path(localFile);

		fs.copyToLocalFile(deleteSrc, distant, local, true);
	}


	/**
	 * Clean folder
	 * @param folder
	 */
	public void deleteFolderContent(String folder) throws IllegalArgumentException, IOException {
		FileStatus[] stats = fs.listStatus(new Path(folder));

		for (FileStatus s : stats) {
			fs.delete(s.getPath(), true);
		}
	}

	/**
	 * delete file given its path
	 * @param folder
	 */
	public void deleteFile(Path file) throws IllegalArgumentException, IOException {
		fs.delete(file, true);
	}

	public void copy(String source, String destination) throws IOException {
		FileUtil.copy(fs, new Path(source), this.fs, new Path(destination), false, true, this.conf);
	}

	public boolean exists(String path) throws IOException {
		return fs.exists(new Path(path));
	}

	public FileSystem getFileSystem() {
		return fs;
	}

	//-----------------------------------------------------------------
	// DATA MEMBERS
	//-----------------------------------------------------------------

	private FileSystem                        fs;
	protected String          			      coreSiteFile;
	protected String          			      baseSiteFile;
	protected Configuration				      conf;
}
