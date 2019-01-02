package com.pbertoni.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * A simple client application that illustrates connecting to
 * a Cassandra cluster. retrieving metadata, creating a schema,
 * loading data into it, and then querying it.
 */
public class SimpleClient
{
	final static int TYPE_SONG = 0;
	final static int TYPE_PLAYLIST = 1;
	
	private Session session;
	public String clusterName;

	public SimpleClient()
	{}

	/**
	 * @brief Connects to the specified node.
	 * @param node a host name or IP address of the node in the cluster
	 */
	public void connect(String node)
	{
		Cluster cluster = Cluster.builder()
				.addContactPoint(node)
				// .withSSL() // uncomment if using client to node encryption
				.build();
		Metadata metadata = cluster.getMetadata();
		session = cluster.connect();
		clusterName = metadata.getClusterName();
	}

	/**
	 * @brief Creates the simplex keyspace and two tables, songs and playlists.
	 * Overloads with default replication factor set to 1.
	 */
	public void createSchema()
	{
		createSchema(1);
	}

	/**
	 * @brief Creates the simplex keyspace and two tables, songs and playlists.
	 * @param replicationFactor the replication factor tho be reached in the cluster
	 */
	public void createSchema(int replicationFactor)
	{
		Integer repl = new Integer(replicationFactor);
		session.execute(
				"CREATE KEYSPACE simplex WITH replication " +
				"= {'class':'SimpleStrategy', 'replication_factor':" + repl.toString() + "};");

		session.execute(
				"CREATE TABLE simplex.songs (" +
					"id uuid PRIMARY KEY," +
					"title text," +
					"album text," +
					"artist text," +
					"tags set<text>," +
					"data blob" +
				");");
		session.execute(
				"CREATE TABLE simplex.playlists (" +
					"id uuid," +
					"title text," +
					"album text, " +
					"artist text," +
					"song_id uuid," +
					"PRIMARY KEY (id, title, album, artist)" +
					");");
	}

	/**
	 * @brief Loads some data into the schema so that we can query the tables.
	 */
	public void loadData()
	{
		session.execute(
				"INSERT INTO simplex.songs (id, title, album, artist, tags) " +
				"VALUES (" +
					"756716f7-2e54-4715-9f00-91dcbea6cf50," +
					"'La Petite Tonkinoise'," +
					"'Bye Bye Blackbird'," +
					"'Joséphine Baker'," +
					"{'jazz', '2013'})" +
				";");
		session.execute(
				"INSERT INTO simplex.songs (id, title, album, artist, tags) " +
				"VALUES (" +
					"f6071e72-48ec-4fcb-bf3e-379c8a696488," +
					"'Die Mösch'," +
					"'In Gold'," +
					"'Willi Ostermann'," +
					"{'kölsch', '1996', 'birds'}" +
				");");
		session.execute(
				"INSERT INTO simplex.songs (id, title, album, artist, tags) " +
				"VALUES (" +
					"fbdf82ed-0063-4796-9c7c-a3d4f47b4b25," +
					"'Memo From Turner'," +
					"'Performance'," +
					"'Mick Jager'," +
					"{'soundtrack', '1991'}" +
				");");
		session.execute(
				"INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
				"VALUES (" +
					"2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
					"756716f7-2e54-4715-9f00-91dcbea6cf50," +
					"'La Petite Tonkinoise'," +
					"'Bye Bye Blackbird'," +
					"'Joséphine Baker'" +
				");");
		session.execute(
				"INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
				"VALUES (" +
					"2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
					"f6071e72-48ec-4fcb-bf3e-379c8a696488," +
					"'Die Mösch'," +
					"'In Gold'," +
					"'Willi Ostermann'" +
				");");
		session.execute(
				"INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
				"VALUES (" +
					"3fd2bedf-a8c8-455a-a462-0cd3a4353c54," +
					"fbdf82ed-0063-4796-9c7c-a3d4f47b4b25," +
					"'Memo From Turner'," +
					"'Performance'," +
					"'Mick Jager'" +
				");");
	}

	/**
	 * @brief Queries the songs and playlists tables for data.
	 */
	public void queryPlaylistSchema()
	{
		ResultSet results = session.execute(
				"SELECT * FROM simplex.playlists " +
				"WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");

		System.out.println("Filtering playlists with id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d");
		printDivider(TYPE_PLAYLIST);
		for (Row row : results)
			printRow(row, TYPE_PLAYLIST);
		System.out.println();
	}

	/**
	 * @brief Updates the songs table with a new song and then queries the table to retrieve data.
	 */
	public void updateSongSchema()
	{
		session.execute(
				"UPDATE simplex.songs " +
				"SET tags = tags + { 'entre-deux-guerres' } " +
				"WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;");

		ResultSet results = session.execute(
				"SELECT * FROM simplex.songs " +
				"WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;");

		System.out.println("Filtering songs with id = 756716f7-2e54-4715-9f00-91dcbea6cf50");
		printDivider(TYPE_SONG);
		for (Row row : results)
			printRow(row, TYPE_SONG);
		System.out.println();
	}

	/**
	 * @brief Drops the specified schema.
	 * @param keyspace the keyspace to drop (and all of its data)
	 */
	public void dropSchema(String keyspace)
	{
		getSession().execute("DROP KEYSPACE " + keyspace);
	}

	/**
	 * @return the current session to execute statements on
	 */
	public Session getSession()
	{
		return this.session;
	}

	/** @brief used by the workaround method in the BoundStatementsclient child class. */
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

	public static void printDivider(int type)
	{
		if(type==TYPE_SONG)
			System.out.println("Songs\n" + 
					String.format("%-30s\t%-20s\t%-20s\t%-40s\t%-40s\n", "title", "album", "artist", "id", "tags"));
		else if(type==TYPE_PLAYLIST)
			System.out.println("Playlists\n" + 
					String.format("%-30s\t%-20s\t%-20s\t%-40s\t%-40s\n", "title", "album", "artist", "id", "song_id"));
	}

	public static void printRow(Row row, int type)
	{
		if (type== TYPE_SONG)
			System.out.println(String.format("%-30s\t%-20s\t%-20s\t%-30s\t%-40s", row.getString("title"), row.getString("album"),
				row.getString("artist"), row.getUUID("id"), row.getSet("tags", String.class).toString()));
		else if(type==TYPE_PLAYLIST)
			System.out.println(String.format("%-30s\t%-20s\t%-20s\t%-30s\t%-30s", row.getString("title"), row.getString("album"),
				row.getString("artist"), row.getUUID("id"), row.getUUID("song_id")));
	}

	/**
	 * @brief Creates simple client application that illustrates connecting to
	 * a Cassandra cluster. retrieving metadata, creating a schema,
	 * loading data into it, and then querying it.
	 * @param args - the first arg is mandatory and is the server URL.
	 */
	public static void main(String[] args)
	{
		final int REPLICATION_FACTOR = 1;
		String schemaName = "simplex";

		if(args.length == 0)
		{
			log("ERROR: SimpleClient needs at least one argument (server URL)");
			return;
		}
		String serverUrl = args[0];

		SimpleClient client = new SimpleClient();
		client.connect(serverUrl);
		log("Connected to cluster " + client.clusterName);

		client.dropSchema(schemaName);
		log(schemaName + " keyspace dropped.");

		try
		{
			client.createSchema(REPLICATION_FACTOR);
			log(schemaName + " keyspace and schema created.");
		}
		catch (com.datastax.driver.core.exceptions.AlreadyExistsException aee)
		{
			log(aee.getMessage());
		}

		client.loadData();
		log("Data loaded.\n");
		client.queryPlaylistSchema();
		client.updateSongSchema();

		client.close();
	}

	public static void log(String message)
	{
		System.out.println("[CLIENT] " + message);
	}
}
