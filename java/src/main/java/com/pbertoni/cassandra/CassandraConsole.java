package com.pbertoni.cassandra;

public class CassandraConsole
{
	/**
	 * @param args[0] - mandatory and is the server URL.
	 * @param args[1] - mandatory and is the action
	 */
	public static void main(String[] args)
	{
		try
		{
			if(args.length == 2)
			{
				CassandraConnector.log("Called with " + args[0] + ", " + args[1]);
				CassandraConnector client = new CassandraConnector(args[0]);
				client.connect();

				if(args[1].compareTo("drop")==0)
				{
					client.dropSchema();
				}
				else if(args[1].compareTo("create")==0)
				{
					client.createSchemaIfNotExists();
				}
				else if(args[1].compareTo("list")==0)
				{
					client.listCheckpoints();
				}

				CassandraConnector.log("Shutting down!");
				client.close();
			}
			else
				CassandraConnector.log("ERROR: Cassandra Connector needs exactly 2 arguments");
		}
		catch(Exception e)
		{
			CassandraConnector.printLongerTrace(e);
		}
		//client.loadData();
	}
}
