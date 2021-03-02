package assign2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Index {
	static Connection myCon=null;
	static PreparedStatement mySt=null;
	static ResultSet myRs=null;
	private static Statement sta;
	
	//connect to postgresql
	public static void connect() {
		
		String url="jdbc:postgresql://127.0.0.1:5432/assign2";
		String user = "postgres";
		String pwd="12345678";
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			myCon=DriverManager.getConnection(url, user, pwd);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("connect successfully!");
	}
	
	public static void index() {
		sta = null;
		try {
			sta=myCon.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String index1="create index movie_index on Movie(id, startYear,runtime);";
		String index2="create index genre_index on Genre(id, genre);";
		String index3="create index moviegenre_index on Movie_Genre(movie);";
		String index4="create index member_index on Member(id, name,deathYear);";
		String index5="create index movieactor_index on Movie_Actor(actor, movie);";
		String index6="create index movieproducer_index on Movie_Producer(producer, movie);";
		String index7="create index role_index on Role(id, role);";
		String index8="create index actormovierole_index on Actor_Movie_Role(actor, role);";
		try {
			
			sta.executeUpdate(index1);
			sta.executeUpdate(index2);
			sta.executeUpdate(index3);
			sta.executeUpdate(index4);
			sta.executeUpdate(index5);
			sta.executeUpdate(index6);
			sta.executeUpdate(index7);
			sta.executeUpdate(index8);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			sta.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("create index successfully!");
	}
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
