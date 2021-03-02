package assign2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

public class Query {
	static Connection myCon=null;
	static PreparedStatement mySt=null;
	static ResultSet myRs=null;
	private static Statement st;
	
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
	
	//execute the 2.1 query
	public static void query1() {
		int i = 0;
		try {
			st=myCon.createStatement();
			myRs=st.executeQuery("select * from Actor_Movie_Role where role is null;");
			//count the number of times the noll-value role appears
			while(myRs.next()) {
				i++;
			}
			System.out.println(i);
//			myRs.close();
			st.close();
//			myCon.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//execute the 2.2 query
	public static void query2() {
		try {
			st=myCon.createStatement();
			myRs=st.executeQuery("select distinct mem.name"+
					" from Movie as mov join Movie_Actor"+
					" on mov.id=movie"+
					" join Member as mem"+
					" on mem.id=actor "+
					" where mem.deathYear=''"+
					" and mem.name like 'Phi%'"+
					" and mem.name !=''"+
					" and mov.startYear is distinct from '2014';");
			while(myRs.next()) {
				String name=myRs.getString("name");
				System.out.println(name);
			}
//			myRs.close();
			st.close();
//			myCon.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//execute the 2.3 query
	public static void query3() {
		try {
			HashMap<String,Integer> hashmap=new HashMap<String,Integer>();
			st=myCon.createStatement();
			myRs=st.executeQuery("select mem.name"+
					" from Movie as mov join Movie_Genre as mg"+
					" on mov.id=mg.movie"+
					" join Genre as gen"+
					" on gen.id=mg.genre"+
					" join Movie_Producer as pro"+
					" on pro.movie=mov.id"+
					" join Member as mem"+
					" on mem.id=pro.producer"+
					" where mov.startYear='2017'"+
					" and mem.name like '%Gill%'"+
					" and gen.genre like '%Talk-Show%';");
			while(myRs.next()) {
				String name=myRs.getString("name");
				//store the name and occurrence number in a hashmap 
				Set<String> wordSet=hashmap.keySet();
				if(wordSet.contains(name)) {
					Integer num=hashmap.get(name);
					num++;
					hashmap.put(name, num);
				}else {
					hashmap.put(name, 1);
				}
			}
			//iterate the hashmap to find which one appears most often
			Iterator<String> iterator=hashmap.keySet().iterator();
			int max=0;
			String most=null;
			while(iterator.hasNext()) {
				String word =iterator.next();
				if(hashmap.get(word)>max) {
					max=hashmap.get(word);
					most=word;
				}
			}
			System.out.println(most+" produced the most talk shows in 2017.");
//			myRs.close();
			st.close();
//			myCon.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//execute the 2.4 query
	public static void query4() {
		try {
			HashMap<String,Integer> hashmap=new HashMap<String,Integer>();
			st=myCon.createStatement();
			myRs=st.executeQuery("select mem.name"+
					" from Member as mem join Movie_Producer"+
					" on mem.id=producer"+
					" join Movie as mov"+
					" on mov.id=movie"+
					" where mem.deathYear=''"+
					" and runtime !=''"+
					" and to_number(mov.runtime,9999999999999999999) >120;");
			while(myRs.next()) {
				String name=myRs.getString("name");
				//store the name and occurrence number in a hashmap 
				Set<String> wordSet=hashmap.keySet();
				if(wordSet.contains(name)) {
					Integer num=hashmap.get(name);
					num++;
					hashmap.put(name, num);
				}else {
					hashmap.put(name, 1);
				}
			}
			//iterate the hashmap to find which one appears most often
			Iterator<String> iterator=hashmap.keySet().iterator();
			int max=0;
			String most=null;
			while(iterator.hasNext()) {
				String word =iterator.next();
				if(hashmap.get(word)>max) {
					max=hashmap.get(word);
					most=word;
				}
			}
			System.out.println(most+" produced greatest.");
			myRs.close();
			st.close();
			myCon.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//execute the 2.5 query
	public static void query5() {
		try {
			st=myCon.createStatement();
			myRs=st.executeQuery("select distinct mem.name"+
					" from Member as mem join Actor_Movie_Role as jar"+
					" on mem.id=jar.actor"+
					" join Role as ro"+
					" on ro.id=jar.role "+
					" where mem.deathYear=''"+
					" and ro.role like '%Jesus%'"+
					" and ro.role like '%Christ%';");
			while(myRs.next()) {
				String name=myRs.getString("name");
				System.out.println(name);
			}
			myRs.close();
			st.close();
			myCon.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Query.connect();
		Query.query1();
		Query.query2();
		Query.query3();
		Query.query4();
		Query.query5();
		

	}

}
