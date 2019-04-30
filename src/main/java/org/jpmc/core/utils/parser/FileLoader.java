package org.jpmc.core.utils.parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.jpmc.core.utils.DateUtil;
import org.jpmc.core.utils.FileProcessor;

/**
 * 
 * @author Sowmith
 * 
 */
public class FileLoader {
	static Logger logger = Logger.getLogger(FileLoader.class.getName());
	private static final 
		String SQL_INSERT = "INSERT INTO ${table}(${keys}) VALUES(${values})";
	private static final String TABLE_REGEX = "\\$\\{table\\}";
	private static final String KEYS_REGEX = "\\$\\{keys\\}";
	private static final String VALUES_REGEX = "\\$\\{values\\}";
	private String regPattern="";
	private String replacePattern="";
	private Connection connection;
	private String dilimiter=null;
	private int totalFixedStrLength=0;
	private String isFixedLengthRun="false";
	public FileLoader(Connection connection) {
		this.connection = connection;
	}
	
	private List<String[]> processInputFile(String inputFilePath) throws Exception {
	    List<String[]> inputList = new ArrayList<String[]>();
	    try{
	      File inputF = new File(inputFilePath);
	      InputStream inputFS = new FileInputStream(inputF);
	      BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));
	      isFixedLengthRun=FileProcessor.prop.getProperty("IS_FIXED_LENGTH_RUN");
	      if(Boolean.parseBoolean(isFixedLengthRun)) {
	    	  generatePattern();
		      if(StringUtils.isEmpty(replacePattern))
		      {
		    	  logger.error("Please configure COLUMN_LENGTHS");
		    	  br.close();
		    	  return null;
		      }
	      }
	      dilimiter=FileProcessor.prop.getProperty("DILIMITER");
	      dilimiter=StringUtils.isNotBlank(dilimiter)?dilimiter:",";
			String rowsCountsToSkip=FileProcessor.prop.getProperty("SKIPPED_ROWS_COUNT");
			rowsCountsToSkip=StringUtils.isNotBlank(rowsCountsToSkip)?rowsCountsToSkip:"0";
	      inputList = br.lines().skip(toInt(rowsCountsToSkip,"SKIPPED_ROWS_COUNT")).map(mapToItem).collect(Collectors.toList());
	      br.close();
	    } catch (IOException e) {
	    }
	    return inputList ;
	}
	
	private Function<String, String[]> mapToItem = (line) -> {
		if(Boolean.parseBoolean(isFixedLengthRun)) {
		  if(line.length()==totalFixedStrLength) {
		  String[] p = line.replaceAll(regPattern,replacePattern).split("#");
		  	return p;
		  }else {
			  logger.error("invalid record...lines starts :"+line.substring(0, 20));
		  return line.replaceAll(regPattern,replacePattern).split("#");
		  }
		}else {
			String[] p = line.split(dilimiter);
		  	return p;
		}
	};
	
	private void generatePattern() {
		String fixedStringLength=FileProcessor.prop.getProperty("COLUMN_LENGTHS");
		if(StringUtils.isBlank(fixedStringLength)) {
			logger.error("Please give fixed file column lengths");
		}
		String[] lengths=fixedStringLength.split(",");
		logger.info("Column lengths Count : "+lengths.length);
		regPattern=regPattern+"^";
		for(int i=0;i<lengths.length;i++) {
			regPattern=regPattern+"(.{"+lengths[i]+"})";
			replacePattern=replacePattern+"$"+(i+1);
			totalFixedStrLength=totalFixedStrLength+Integer.parseInt(lengths[i]);
			if(i != lengths.length-1)
				replacePattern=replacePattern+"#";
		}
		regPattern=regPattern+".*";
	}
	
	public void fileProcesser(String file, String tableName,
			boolean truncateBeforeLoad) throws Exception {
		
		List<String[]> rows=null;
		String[] headerRow=null;
		int count = 0;
		if(null == this.connection) {
			throw new Exception("Not a valid connection.");
		}
		try {
				rows=processInputFile(file);
				String isHeaderAvailable=FileProcessor.prop.getProperty("HEADER_ROW_NUMBER");
				isHeaderAvailable=StringUtils.isNotBlank(isHeaderAvailable)?isHeaderAvailable:null;
				if(StringUtils.isBlank(isHeaderAvailable)) {
					String columns=FileProcessor.prop.getProperty("COLUMNS");
					if(StringUtils.isBlank(columns)) {
						logger.error("Please configure the COLUMS as HEADER_ROW_NOT AVAILABLE");
						return;
					}else {
						headerRow=columns.split(",");
					}

				}else {
					headerRow=rows.get(toInt(isHeaderAvailable,"HEADER_ROW_NUMBER"));
					count=toInt(isHeaderAvailable,"HEADER_ROW_NUMBER")+1;
				}
				
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error while processing ");
			throw new Exception("Error occured while processing file. "
					+ e.getMessage());
		}
		

		String questionmarks = StringUtils.repeat("?,", headerRow.length);
		questionmarks = (String) questionmarks.subSequence(0, questionmarks
				.length() - 1);

		String query = SQL_INSERT.replaceFirst(TABLE_REGEX, tableName);
		query = query
				.replaceFirst(KEYS_REGEX, StringUtils.join(headerRow, ","));
		logger.debug("header colums  length :"+headerRow.length);

		query = query.replaceFirst(VALUES_REGEX, questionmarks);

		System.out.println("Query: " + query);

		databaseLoad(truncateBeforeLoad, tableName, rows, count, query,headerRow.length);
	}

	private void databaseLoad(boolean truncateBeforeLoad, String tableName, List<String[]> rows, int count, String query,int length)
			throws SQLException, Exception {
		String[] nextLine;
		Connection con = null;
		PreparedStatement ps = null;
		int completedRows=0;
		try {
			con = this.connection;
			con.setAutoCommit(false);
			ps = con.prepareStatement(query);
			if(truncateBeforeLoad) {
				//delete data from table before loading csv
				con.createStatement().execute("DELETE FROM " + tableName);
			}
			logger.info("Processing Start Time : "+DateUtil.convertToString(new Date()));
			logger.info("Total rows to process : "+(rows.size()-count));
			final int batchSize = 1000;			
			Date date = null;
			while ((count != rows.size() &&  (nextLine = rows.get(count)) != null)) {
				if (null != nextLine) {
					int index = 1;
					/*logger.info("====>"+StringUtils.join(nextLine,","));
					logger.info("<===");*/
					if(nextLine.length >= length) {
						for (int j = 0; j < length; j++) {
							//date = DateUtil.convertToDate(string);
							String string = nextLine[j];
							if (null != date) {
								ps.setDate(index++, new java.sql.Date(date
										.getTime()));
							} else {
								ps.setString(index++, string.trim());
							}
						}
						completedRows++;
						ps.addBatch();
					}
					/*else{
						logger.debug("skipped rows :"+nextLine[1]););
						}*/

					//ps.addBatch();
				}
				if (++count % batchSize == 0) {
					ps.executeBatch();
				}
			}
			ps.executeBatch(); // insert remaining records
			con.commit();
			logger.info("Total Completed rows : "+completedRows);
			logger.info("Processing Completed Time : "+DateUtil.convertToString(new Date()));
			logger.info("Data Upload Completed Successfully");
		} catch (Exception e) {
			con.rollback();
			e.printStackTrace();
			throw new Exception(
					"Error occured while loading data from file to database."
							+ e.getMessage());
		} finally {
			if (null != ps)
				ps.close();
			if (null != con)
				con.close();
		}
	}

	private int toInt(String name,String property) throws Exception {
		try {
			return Integer.parseInt(name);
		}
		catch(Exception e) {
			logger.error(property+" Wrong type.Please Enter Number");
			throw new Exception("Error occured while processing file. "
					+ e.getMessage());
		}
	}

}
