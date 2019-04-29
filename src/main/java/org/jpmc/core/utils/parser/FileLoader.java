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
	public FileLoader(Connection connection) {
		this.connection = connection;
	}
	
	private List<String[]> processInputFile(String inputFilePath) throws Exception {
	    List<String[]> inputList = new ArrayList<String[]>();
	    try{
	      File inputF = new File(inputFilePath);
	      InputStream inputFS = new FileInputStream(inputF);
	      BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));
	    /*  generatePattern();
	      if(StringUtils.isEmpty(replacePattern))
	      {
	    	  logger.error("Please configure COLUMN_LENGTHS");
	    	  br.close();
	    	  return null;
	      }*/
	      dilimiter=FileProcessor.prop.getProperty("DILIMITER");
	      dilimiter=StringUtils.isNotBlank(dilimiter)?dilimiter:",";
	      String headerRows=FileProcessor.prop.getProperty("FILE_HEADER_AVAILABLE");
	      headerRows=StringUtils.isNotBlank(headerRows)?headerRows:"0";
	      inputList = br.lines().skip(toInt(headerRows,"FILE_HEADER_AVAILABLE")).map(mapToItem).collect(Collectors.toList());
	      br.close();
	    } catch (IOException e) {
	    }
	    return inputList ;
	}
	
	private Function<String, String[]> mapToItem = (line) -> {
		 String[] p = line.split(dilimiter);
		 /*
		  String[] p = line.replaceAll(regPattern,replacePattern).split("-");*/
		  return p;
	};
	
	private void generatePattern() {
		String fixedStringLength=FileProcessor.prop.getProperty("COLUMN_LENGTHS");
		if(StringUtils.isBlank(fixedStringLength)) {
			logger.error("Please give fixed file column lengths");
		}
		String[] lengths=fixedStringLength.split(",");
		regPattern=regPattern+"^";
		for(int i=0;i<lengths.length;i++) {
			regPattern=regPattern+"(.{"+lengths[i]+"})";
			replacePattern=replacePattern+"$"+(i+1);
			if(i != lengths.length-1)
				replacePattern=replacePattern+"-";
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
					String rowsCountsToSkip=FileProcessor.prop.getProperty("SKIPPED_ROWS_COUNT");
					isHeaderAvailable=StringUtils.isNotBlank(rowsCountsToSkip)?rowsCountsToSkip:"0";
					count=toInt(rowsCountsToSkip,"SKIPPED_ROWS_COUNT")+1;
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
		query = query.replaceFirst(VALUES_REGEX, questionmarks);

		System.out.println("Query: " + query);

		databaseLoad(truncateBeforeLoad, tableName, rows, count, query);
	}

	private void databaseLoad(boolean truncateBeforeLoad, String tableName, List<String[]> rows, int count, String query)
			throws SQLException, Exception {
		String[] nextLine;
		Connection con = null;
		PreparedStatement ps = null;
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
					for (String string : nextLine) {
						//date = DateUtil.convertToDate(string);
						if (null != date) {
							ps.setDate(index++, new java.sql.Date(date
									.getTime()));
						} else {
							ps.setString(index++, string.trim());
						}
					}
					ps.addBatch();
				}
				if (++count % batchSize == 0) {
					ps.executeBatch();
				}
			}
			ps.executeBatch(); // insert remaining records
			con.commit();
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
