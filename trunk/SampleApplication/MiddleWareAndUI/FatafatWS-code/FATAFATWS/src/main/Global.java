package main;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
//import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;

import main.DataServices.DBConnectionData;;

//@WebServlet
public class Global extends HttpServlet 
{
	private static final long serialVersionUID = 7057088960896734291L;

	public static DBConnectionData CONNECTION_DATA;
	
	public static String CONFIG_COMPANY_NAME;
	public static final String DEFAULT_COMPANY_NAME = "default";
	public static String CONFIG_LOG_FILE_PATH = "./default.log";
	public static String CONFIG_LOG_FILE_PATTERN;
	
	public static final String commaSplitRegEx = "(\\s*,\\s*)+";
	
	public Global() 
	{
		super();
	}
		
	
	public String getDataConnectionString()
	{
		return getServletConfig().getInitParameter("DataConnectionString");
	}
	
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		try {
			CONNECTION_DATA = DataServices.parseConnectionString(getServletConfig().getInitParameter("DataConnectionString"));
//				DIMDATA_CONNECTION_DATA = DataServices.parseConnectionString(getServletConfig().getInitParameter("DIMDataConnectionString"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		try {
			CONFIG_COMPANY_NAME = getServletConfig().getInitParameter("ClientName"); 
		} catch (Exception e) {}
		
		try {
			//CONFIG_LOG_FILE_PATH = getServletConfig().getInitParameter("LogFilePath"); 
		} catch (Exception e) {}
		
		try {
			//CONFIG_LOG_FILE_PATTERN = getServletConfig().getInitParameter("LogFilePattern"); 
		} catch (Exception e) {}
		
	}
	
}
