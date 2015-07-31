package com.ligadata.kamanja.consumer.util;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;

public class ConfigManager {

	private static Logger logger = Logger.getLogger(ConfigManager.class);

	public static XMLConfiguration getConfig(String configFile) {

		XMLConfiguration config = null;
		try {
			config = new XMLConfiguration(configFile);
		} catch (ConfigurationException e) {
			logger.error("Error while reading xonfiguration file", e);
		}

		return config;
	}

}
