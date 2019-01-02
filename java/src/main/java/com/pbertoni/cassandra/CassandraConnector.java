package com.pbertoni.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class CassandraConnector
{
	private final static int RECEIVING_SOCKET = 4444;
	private final static int BUFFER_BYTE_SIZE = 102400;
	private final static int BYTES_NEEDED = 128;
	private final static int BYTES_CODE = 4;

	private Session session;
	private Cluster cluster;

	private String serverUrl;
	private String appName = "FooAppName";
	private String ckpId = "FooCkpId";
	private String compName = "FooCompName";

	private final static String keyspaceName = "blockmon";
	private final static String tableName = "checkpoints";
	private final static String canonicalName = keyspaceName + "." + tableName;
	/** TODO maybe astract */
	private final static int replicationFactor = 1;
	private byte[] stateBuffer;
	private int stateLength;

	public CassandraConnector(String serverUrl)
	{
		this.serverUrl = serverUrl;
	}

	/**
	 * Connects to the specified node.
	 * @param node a host name or IP address of the node in the cluster
	 */
	public void connect()
	{
		stateLength = 0;
		cluster = Cluster.builder()
				.addContactPoint(this.serverUrl)
				// .withSSL() // uncomment if using client to node encryption
				.build();
		session = cluster.connect();

		if(session == null)
			throw new IllegalStateException("Session is null. Problems connecting to " + this.serverUrl);
		else
			log("Connected to " + this.serverUrl + " for cluster " + cluster.getMetadata().getClusterName());
	}

	/**
	 * @brief creates
	 * <ol>
	 * 		<li> 'blockmon' keyspace
	 * 		<li> 'checkpoints' table
	 * </ol>
	 */
	public void createSchemaIfNotExists()
	{
		createKeyspaceIfNotExists();
		createTableIfNotExists();
	}

	public void createKeyspaceIfNotExists()
	{
		if (cluster.getMetadata().getKeyspace(keyspaceName) == null)
		{
			log("create new keyspace `" + keyspaceName + "`");

			session.execute(
					"CREATE KEYSPACE " + keyspaceName + " WITH replication " +
					"= {'class':'SimpleStrategy', 'replication_factor':" +
					(new Integer(replicationFactor).toString()) + "};");
		}
		//else
			//log("keyspace `" + keyspaceName + "` already exists");
	}

	public void createTableIfNotExists()
	{
		// warning: datastax javadoc says 'false' instead of correct 'null' here
		if (cluster.getMetadata().getKeyspace(keyspaceName).getTable(tableName) == null)
		{
			log("create new table `" + tableName + "` into keyspace `" + keyspaceName + "`");
			session.execute(
					"CREATE TABLE " + canonicalName + " (" +
							"application_name text," +
							"composition_name text," +
							"checkpoint_id text," +
							"state blob," +
							"PRIMARY KEY (application_name, composition_name, checkpoint_id)" +
					");");
		}
		//else
			//log("table `" + tableName + "` already exists");
	}

	/**
	 * @brief Loads some data into the schema so that we can query the tables.
	 */
	public void loadData()
	{
		PreparedStatement prep = session.prepare(
				"insert into " + canonicalName + " (application_name, state, checkpoint_id, composition_name) "
						+ "values ('" + appName + "', ?, '" + ckpId + "', '" + compName + "');");

		/*
		for(int i = 0; i < size; i++)
			System.out.printf("%02X", buffer[i]);
		System.out.println("\n");
		 */

		ByteBuffer byteBuffer = ByteBuffer.wrap(stateBuffer, 0, this.stateLength);

		session.execute(prep.bind(byteBuffer));
		log(this.stateLength + " bytes inserted for app `" + appName + "`, ckp `" + ckpId + "`, comp `" + compName + "`");
	}

	/**
	 * @brief Returns the current session.
	 * @return the current session to execute statements on
	 */
	public Session getSession()
	{
		return this.session;
	}

	/**
	 * @brief used by the workaround method in the BoundStatementsclient child class.
	 * @param session
	 */
	void setSession(Session session)
	{
		this.session = session;
	}

	/**
	 * @brief Shuts down the session and its cluster.
	 */
	public void close()
	{
		session.shutdown();
		session.getCluster().shutdown();
	}

	/**
	 * @brief Put this Node in a wait state, for any connection of socket RECEIVING_SOCKET.
	 * When a connection arrives, read from a buffer
	 * <ol>
	 * 	<li> compName
	 * </ol>
	 */
	public void receiveStateOnSocket()
	{
		int bytesToRead = 0;
		ServerSocket sockAccept;
		Socket socket;
		InputStream is = null;
		OutputStream os = null;

		try
		{
			log("Waiting for client requests on socket " + RECEIVING_SOCKET + ".....");
			sockAccept = new ServerSocket(RECEIVING_SOCKET);
			socket = sockAccept.accept();
			is = socket.getInputStream();
			os = socket.getOutputStream();

			byte[] byteBuffer = new byte[BUFFER_BYTE_SIZE];

			try
			{
				try
				{
					// read first number of bytes to transfer and composition name
					int bytesNeeded = BYTES_NEEDED;
					while(true)
					{
						int read = is.read(byteBuffer, BYTES_NEEDED - bytesNeeded, bytesNeeded);
						bytesNeeded = bytesNeeded - read;
						if(bytesNeeded == 0)
							break;
					}

					bytesToRead = (byteBuffer[0]) & 0xFF;
					bytesToRead = bytesToRead << 8 | (byteBuffer[1] & 0xFF);
					bytesToRead = bytesToRead << 8 | (byteBuffer[2] & 0xFF);
					bytesToRead = bytesToRead << 8 | (byteBuffer[3] & 0xFF);

					int cursor = 0;
					int compEnd = lookupString(cursor, byteBuffer);

					compName = new String(byteBuffer, BYTES_CODE, compEnd, "UTF-8");
					//compName = new String(bytebuffer, BYTES_CODE, BYTES_NEEDED-BYTES_CODE, "UTF-8");
					log("Received composition `" + compName + "`");

					//  compositionName = new String(bytebuffer, 4, 124, "UTF-8");

					bytesNeeded = bytesToRead;
					stateBuffer = new byte[bytesNeeded];

					int offset = 0;

					while(bytesNeeded > 0)
					{
						//int read = is.read(bytebuffer, offset, bytesNeeded);
						int read = is.read(stateBuffer, offset, bytesNeeded);
						bytesNeeded = bytesNeeded - read;
						offset = offset + read;
					}

					byte[] retVal = new byte[1];
					retVal[0] = '1';

					os.write(retVal, 0, 1);
					log("Received on socket composition `" + compName + "`");
				}
				finally
				{
					is.close();
					socket.close();
					sockAccept.close();
				}
			}
			catch (IOException e)
			{
				System.err.println("IO failed.");
				System.exit(1);
			}
		}
		catch (IOException e)
		{
			System.err.println("Accept failed.");
			System.exit(1);
		}
		this.stateLength = bytesToRead;
	}

	public int getStateLength()
	{
		return this.stateLength;
	}

	/** Walks with a cursor along an array of bytes, stopping whenever one of this conditions is reached
	 * <ul>
	 * 	<li> cursor < BYTES_NEEDED-BYTES_CODE
	 * 	<li> byteBuffer[cursor + BYTES_CODE] != 0)
	 * </ul>
	 * @param cursor
	 * @param byteBuffer
	 * @return
	 */
	public int lookupString(int cursor, byte[] byteBuffer)
	{
		while ( (cursor < BYTES_NEEDED-BYTES_CODE) && (byteBuffer[cursor + BYTES_CODE] != 0) )
			cursor++;
		return cursor;
	}

	public void querySchema()
	{
		/*	System.out.println("querySchema() 1------------------------------------");

		ResultSet resultsFoo = session.execute(
				"SELECT * FROM system.schema_keyspaces" +
				"WHERE keyspace_name = 'blockmon';");
		System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
				"-------------------------------+-----------------------+--------------------"));
		for (Row row : resultsFoo)
		{
			System.out.println(String.format("%-30s\t%-20s\t%-20s", row.get,
					row.getString("album"),  row.getString("artist")));
		}

		System.out.println("querySchema() 2------------------------------------");

		ResultSet results = session.execute(
				"SELECT * FROM blockmon.checkpointed_states_new;");
		System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "appName", "compName", "ckpId",
				"-------------------------------+-----------------------+--------------------"));
		for (Row row : results)
		{
			System.out.println(String.format("%-30s\t%-20s\t%-20s",
					row.getString("application_name"),
					row.getString("composition_name"),
					row.getString("checkpoint_id")));
		}
		System.out.println();*/
	}

	public void listCheckpoints()
	{
		log("Listing all checkpoints saved into Cassandra Cluster");

		try
		{
			ResultSet results = session.execute("SELECT * FROM " + canonicalName + ";");
			log(String.format("%-30s\t%-20s\t%-20s\n", "appName", "compName", "ckpId"));
			log("");
			for (Row row : results)
			{
				log(String.format("%-30s\t%-20s\t%-20s",
						row.getString("application_name"),
						row.getString("composition_name"),
						row.getString("checkpoint_id")));
			}
			log("");
		}
		catch(com.datastax.driver.core.exceptions.InvalidQueryException iqe)
		{
			log("Query failed. Maybe Schema is messed up in the cluster?");
		}
	}

	public void dropSchema()
	{
		getSession().execute("DROP KEYSPACE " + keyspaceName);
		log("Keyspace dropped");
	}

	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	/**
	 * @brief Creates  simple client application that illustrates
	 * <ol>
	 * 		<li> connecting to a Cassandra cluster
	 *  	<li> retrieving metadata
	 *  	<li> creating a schema
	 * 		<li> loading data into it
	 * 		<li> querying it
	 * </ol>
	 * @param args - the first arg is mandatory and is the server URL.
	 */
	public static void main(String[] args)
	{
		if(args.length == 0)
		{
			log("ERROR: Cassandra Connector needs at least one argument (server URL)");
		}
		else
		{
			while(true)
			{
				CassandraConnector client = new CassandraConnector(args[0]);
	
				try
				{
					client.connect();
					client.createSchemaIfNotExists();
					client.listCheckpoints();
					client.receiveStateOnSocket();
					log("Received a state of " + client.getStateLength() + " bytes");
					client.loadData();
					log("Shutting down");
					client.close();
				}
				catch(Exception e)
				{
					printLongerTrace(e);
				}
			}
		}
	}

	static void printLongerTrace(Throwable t)
	{
		for(StackTraceElement e: t.getStackTrace())
			System.out.println("[ST]  " + e);
	}

	public static void log(String message)
	{
		System.out.println("[CASSANDRA] " + message);
	}
}
