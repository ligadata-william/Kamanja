package main;

import org.joda.time.DateTime;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) 
	{
		MessagesService ms = new MessagesService();
		String s = ms.getEventsInfo(null,true,DateTime.now());
		System.out.println(s);

	}

}
