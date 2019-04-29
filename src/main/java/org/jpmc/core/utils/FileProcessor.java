package org.jpmc.core.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.jpmc.core.utils.databse.DatSource;
import org.jpmc.core.utils.parser.FileLoader;

public class FileProcessor {

static Logger logger = Logger.getLogger(FileProcessor.class.getName());
public static Properties prop=new Properties();
	public static void main(String[] args) {
		if(null != args && args.length > 0 && StringUtils.isNotBlank(args[0])) {
			logger.info("Config location :"+args[0]);
			getConfiguration(args[0]);
		}else {
			logger.info("Please check Config location ");
			return;
		}
		
		try {
			FileLoader loader1 = new FileLoader(DatSource.getConnection());
			String truncateBeforeLoad=FileProcessor.prop.getProperty("TRUNCATE_BEFORE_LOAD");
			truncateBeforeLoad=StringUtils.isNotBlank(truncateBeforeLoad)?truncateBeforeLoad:"false";
			loader1.fileProcesser(FileProcessor.prop.getProperty("FILE_LOCATION"), FileProcessor.prop.getProperty("TABLE_NAME"), Boolean.parseBoolean(truncateBeforeLoad));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	private static void getConfiguration(String location) {
		logger.info("Loading Configuration...");
		try (InputStream input = new FileInputStream(location)) {
	        // load a properties file
	        prop.load(input);
	        logger.info(" Loading Configuration Completed.");
	    } catch (IOException ex) {
	    	logger.info("Error While Loading Configuration...");
	        ex.printStackTrace();
	        
	    }
	}
	

}
