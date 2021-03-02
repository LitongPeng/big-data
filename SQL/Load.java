package assign2;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

public class Load {
	
	static Connection myCon=null;
	static PreparedStatement mySt=null;
	static ResultSet myRs=null;
	private static Statement st1;
	private static Statement myS;
	
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
	
	//create the tables
	public static void table() {
		myS = null;
		try {
			myS=myCon.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String strMovie="create table Movie"+
				"(id text,"+
				"type text,"+
				"title text,"+
				"originalTitle text,"+
				"startYear text,"+
				"endYear text,"+
				"runtime text,"+
				"avgRating text,"+
				"primary key(id));";
		String strGenre="create table Genre"+
				"(id serial,"+
				"genre text,"+
				"primary key(id));";
		String strMovie_Genre="create table Movie_Genre"+
				"(genre int,"+
				"movie text,"+
				"primary key(genre,movie),"+
				"foreign key(genre) references Genre(id),"+
				"foreign key(movie) references Movie(id));";
		String strMember="create table Member"+
				"(id text,"+
				"name text,"+
				"birthYear text,"+
				"deathYear text,"+
				"primary key(id));";
		String strMovie_Actor="create table Movie_Actor"+
				"(actor text,"+
				"movie text,"+
				"primary key(actor,movie),"+
				"foreign key(actor) references Member(id),"+
				"foreign key(movie) references Movie(id));";
		String strMovie_Writer="create table Movie_Writer"+
				"(writer text,"+
				"movie text,"+
				"primary key(writer,movie),"+
				"foreign key(writer) references Member(id),"+
				"foreign key(movie) references Movie(id));";
		String strMovie_Director="create table Movie_Director"+
				"(director text,"+
				"movie text,"+
				"primary key(director,movie),"+
				"foreign key(director) references Member(id),"+
				"foreign key(movie) references Movie(id));";
		String strMovie_Producer="create table Movie_Producer"+
				"(producer text,"+
				"movie text,"+
				"primary key(producer,movie),"+
				"foreign key(producer) references Member(id),"+
				"foreign key(movie) references Movie(id));";
		String strRole="create table Role"+
				"(id serial,"+
				"role text,"+
				"primary key(id));";
		String strActor_Movie_Role="create table Actor_Movie_Role"+
				"(actor text,"+
				"movie text,"+
				"role integer,"+
				"primary key(actor,movie),"+
				"foreign key(actor,movie) references Movie_Actor(actor,movie),"+
				"foreign key(role) references Role(id));";
		try {
//			myS.executeUpdate(strMovie);
//			myS.executeUpdate(strGenre);
//			myS.executeUpdate(strMovie_Genre);
//			myS.executeUpdate(strMember);
//			myS.executeUpdate(strMovie_Actor);
//			myS.executeUpdate(strMovie_Writer);
//			myS.executeUpdate(strMovie_Director);
//			myS.executeUpdate(strMovie_Producer);
//			myS.executeUpdate(strRole);
			myS.executeUpdate(strActor_Movie_Role);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			myS.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("create table successfully!");
	}
	
	//load data for genre table
	public static void loadGenre() {
		int i=0;
		String loadGenre="insert into genre VALUES(default,?)";
		boolean b1 = false;
		try {
			mySt=myCon.prepareStatement(loadGenre);
		} catch (SQLException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		try {
			st1 = myCon.createStatement();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
				InputStream gzipStream = null;
				try {
					gzipStream = new GZIPInputStream(new FileInputStream("/Users/penglitong/Desktop/title.basics.tsv.gz"));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Scanner sc=new Scanner(gzipStream,"UTF-8");
				sc.nextLine();
				while(sc.hasNextLine()) {
					String[] scsplit = sc.nextLine().split("\t");
					String a = scsplit[8]; //a:Genre genre
					String sqla = "SELECT * FROM genre WHERE genre = '" + a+"'"; //find every different genre
					try {
						myRs = st1.executeQuery(sqla);
						b1 = myRs.next();
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					if (b1 == true) {
						continue;
					}
						try {
							mySt.setString(1, a);// if there is no same genre, then add it
					mySt.executeUpdate();	
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
				}
				try {
					mySt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("load data for genre table successfully!");
	}
	
	//load data for member table
	public static void loadMember() {
		int i=0;
		String loadMember="insert into member VALUES(?,?,?,?)";
		try {
			mySt=myCon.prepareStatement(loadMember);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
				InputStream gzipStream = null;
				try {
					gzipStream = new GZIPInputStream(new FileInputStream("/Users/penglitong/Desktop/name.basics.tsv.gz"));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Scanner sc=new Scanner(gzipStream,"UTF-8");
				sc.nextLine();
				while(sc.hasNextLine()) {
					String[] scsplit = sc.nextLine().split("\t");
					String a = scsplit[0]; //Member id
					String b = scsplit[1]; //Member name
					String c = scsplit[2]; //Member birthYear
					String d = scsplit[3]; //Member deathYear
					try {
						mySt.setString(1, a);
						mySt.setString(2, b);
						mySt.setString(3, c);
						mySt.setString(4, d);
						mySt.addBatch();
						i++;
						if(i%100 == 0 ) {
							mySt.executeBatch();
						}
						
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					
				}
				try {
					mySt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("load data for member table successfully!");
	}
	
	//create 3 tables for providing the data for movie table
	public static void tableMovie() {
		Statement myS = null;
		try {
			myS=myCon.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String strAkas="create table strAkas"+
				"(id text,"+
				"type text,"+
				"title text);";
		String strBasics="create table strBasics"+
				"(id text,"+
				"originalTitle text,"+
				"startYear text,"+
				"endYear text,"+
				"runtime text);";
		String strRatings="create table strRatings"+
				"(id text,"+
				"avgRating text);";
		try {
						myS.executeUpdate(strAkas);
						myS.executeUpdate(strBasics);
						myS.executeUpdate(strRatings);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			myS.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("create tablemovie successfully!");
	}
	
	//load data for strAkas table
	public static void load1() {
		int i=0;
		String load1="insert into strAkas VALUES(?,?,?)";
		try {
			mySt=myCon.prepareStatement(load1);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
				InputStream gzipStream = null;
				try {
					gzipStream = new GZIPInputStream(new FileInputStream("/Users/penglitong/Desktop/title.akas.tsv.gz"));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Scanner sc=new Scanner(gzipStream,"UTF-8");
				sc.nextLine();
				while(sc.hasNextLine()) {
					String[] scsplit = sc.nextLine().split("\t");
					String a = scsplit[0]; //Movie id
					String b = scsplit[5]; //Movie type
					String c = scsplit[2]; //Movie title
					try {
						mySt.setString(1, a);
						mySt.setString(2, b);
						mySt.setString(3, c);
						mySt.addBatch();
						i++;
						if(i%100 == 0 ) {
							mySt.executeBatch();
						}
						
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
				try {
					mySt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("load data for strAkas table successfully!");
	}
	
	//load data for strBasics table
	public static void load2() {
		int i=0;
		String load2="insert into strBasics VALUES(?,?,?,?,?)";
		try {
			mySt=myCon.prepareStatement(load2);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
				InputStream gzipStream = null;
				try {
					gzipStream = new GZIPInputStream(new FileInputStream("/Users/penglitong/Desktop/title.basics.tsv.gz"));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Scanner sc=new Scanner(gzipStream,"UTF-8");
				sc.nextLine();
				while(sc.hasNextLine()) {
					String[] scsplit = sc.nextLine().split("\t");
					String a = scsplit[0]; //Movie id
					String b = scsplit[3]; //Movie originalTitle
					String c = scsplit[5]; //Movie startYear
					String d = scsplit[6]; //Movie endYear
					String e = scsplit[7]; //Movie runtime
					try {
						mySt.setString(1, a);
						mySt.setString(2, b);
						mySt.setString(3, c);
						mySt.setString(4, d);
						mySt.setString(5, e);
						mySt.addBatch();
						i++;
						if(i%100 == 0 ) {
							mySt.executeBatch();
						}
						
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
				try {
					mySt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("load data for strBasics table successfully!");
	}
	
	//load data for strRatings table
	public static void load3() {
		int i=0;
		String load3="insert into strRatings VALUES(?,?)";
		try {
			mySt=myCon.prepareStatement(load3);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
				InputStream gzipStream = null;
				try {
					gzipStream = new GZIPInputStream(new FileInputStream("/Users/penglitong/Desktop/title.ratings.tsv.gz"));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Scanner sc=new Scanner(gzipStream,"UTF-8");
				sc.nextLine();
				while(sc.hasNextLine()) {
					String[] scsplit = sc.nextLine().split("\t");
					String a = scsplit[0]; //Movie id
					String b = scsplit[1]; //Movie aveRating
					try {
						mySt.setString(1, a);
						mySt.setString(2, b);
						mySt.addBatch();
						i++;
						if(i%100 == 0 ) {
							mySt.executeBatch();
						}
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
				try {
					mySt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("load data for strRatings table successfully!");
	}
	
	//create a table to combine strAkas table and strBasics table
	public static void table2() {
		Statement myS = null;
		try {
			myS=myCon.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String t2="create table table2"+
				"(id text,"+
				"type text,"+
				"title text,"+
				"originalTitle text,"+
				"startYear text,"+
				"endYear text,"+
				"runtime text);";
		try {
						myS.executeUpdate(t2);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			myS.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("create table2 successfully!");
	}
	
	//let strAkas table join strBasics table
	public static void combine1() {
		String con1="insert into table2 select distinct a.id, a.type, a.title, b.originalTitle, b.startYear, b.endYear,b.runtime"
				+ " from strAkas a left join strBasics b on a.id=b.id";
		try {
			mySt = myCon.prepareStatement(con1);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			mySt.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("combine1 done!");
	}
	
	//let table2 join strRatings table, which is Movie table
	public static void combine2() {
		String con1="insert into Movie select distinct on (a.id) a.id, a.type, a.title, a.originalTitle, a.startYear, a.endYear, a.runtime, b.avgRating"
				+ " from table2 a left join strRatings b on a.id=b.id";
		try {
			mySt = myCon.prepareStatement(con1);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			mySt.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("combine2 done!");
	}
	
	//load data for actors table
	public static void actors() {
		String actors="insert into Movie_Actor VALUES(?,?)";
		int i=0;
		boolean b1 = true;
		boolean b2 = true;
		try {
			mySt=myCon.prepareStatement(actors);
		} catch (SQLException e11) {
			// TODO Auto-generated catch block
			e11.printStackTrace();
		}
				InputStream gzipStream = null;
				try {
					gzipStream = new GZIPInputStream(new FileInputStream("/Users/penglitong/Desktop/title.principals.tsv.gz"));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Scanner sc=new Scanner(gzipStream,"UTF-8");
				sc.nextLine();
				Statement st1 = null;
				try {
					st1 = myCon.createStatement();
				} catch (SQLException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
				while(sc.hasNextLine()) {
					String[] scsplit = sc.nextLine().split("\t");
					String a = scsplit[2]; //nconst
					String b = scsplit[0]; //tconst
					String c = scsplit[3]; //category
					String sqla = "SELECT * FROM member WHERE id = '" + a+"'"; //actors should have Member id
					String sqlb = "SELECT * FROM movie WHERE id = '" + b+"'"; //Movie should have Movie id
					try {
						myRs = st1.executeQuery(sqla);
						b1 = myRs.next();
						myRs = st1.executeQuery(sqlb);
						b2 = myRs.next();
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					if (b1 == false || b2 == false) {
						continue;
					}
					if ("actor".equals(c)){
					
						try {
							mySt.setString(1, a);
							mySt.setString(2, b);
							mySt.addBatch();
							i++;
							if(i%10000 == 0 ) {
								mySt.executeBatch();
							}
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					
				}
				try {
					mySt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("load data for actors table successfully!");
	}
	
	//load data for writers table
	public static void writers() {
		String writers="insert into Movie_Writer VALUES(?,?)";
		int i=0;
		boolean b1 = true;
		boolean b2 = true;
		try {
			mySt=myCon.prepareStatement(writers);
		} catch (SQLException e11) {
			// TODO Auto-generated catch block
			e11.printStackTrace();
		}
				InputStream gzipStream = null;
				try {
					gzipStream = new GZIPInputStream(new FileInputStream("/Users/penglitong/Desktop/title.principals.tsv.gz"));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Scanner sc=new Scanner(gzipStream,"UTF-8");
				sc.nextLine();
				Statement st1 = null;
				try {
					st1 = myCon.createStatement();
				} catch (SQLException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
				while(sc.hasNextLine()) {
					String[] scsplit = sc.nextLine().split("\t");
					String a = scsplit[2]; //nconst
					String b = scsplit[0]; //tconst
					String c = scsplit[3]; //category
					String sqla = "SELECT * FROM member WHERE id = '" + a+"'"; //writer should have Member id
					String sqlb = "SELECT * FROM movie WHERE id = '" + b+"'"; //Movie should have Movie id
					try {
						myRs = st1.executeQuery(sqla);
						b1 = myRs.next();
						myRs = st1.executeQuery(sqlb);
						b2 = myRs.next();
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					if (b1 == false || b2 == false) {
						continue;
					}
					if ("writer".equals(c)){
					
						try {
							mySt.setString(1, a);
							mySt.setString(2, b);
							mySt.addBatch();
							i++;
							if(i%10000 == 0 ) {
								mySt.executeBatch();
							}
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					
				}
				try {
					mySt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("load data for writers table successfully!");
	}
	
	//load data for directors table
	public static void directors() {
		String directors="insert into Movie_Director VALUES(?,?)";
		int i=0;
		boolean b1 = true;
		boolean b2 = true;
		try {
			mySt=myCon.prepareStatement(directors);
		} catch (SQLException e11) {
			// TODO Auto-generated catch block
			e11.printStackTrace();
		}
				InputStream gzipStream = null;
				try {
					gzipStream = new GZIPInputStream(new FileInputStream("/Users/penglitong/Desktop/title.principals.tsv.gz"));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Scanner sc=new Scanner(gzipStream,"UTF-8");
				sc.nextLine();
				Statement st1 = null;
				try {
					st1 = myCon.createStatement();
				} catch (SQLException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
				while(sc.hasNextLine()) {
					String[] scsplit = sc.nextLine().split("\t");
					String a = scsplit[2]; //nconsst
					String b = scsplit[0]; //tconst
					String c = scsplit[3]; //category
					String sqla = "SELECT * FROM member WHERE id = '" + a+"'"; //directors should have Member id
					String sqlb = "SELECT * FROM movie WHERE id = '" + b+"'"; //Movie should have Movie id
					try {
						myRs = st1.executeQuery(sqla);
						b1 = myRs.next();
						myRs = st1.executeQuery(sqlb);
						b2 = myRs.next();
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					if (b1 == false || b2 == false) {
						continue;
					}
					if ("director".equals(c)){
					
						try {
							mySt.setString(1, a);
							mySt.setString(2, b);
							mySt.addBatch();
							i++;
							if(i%10000 == 0 ) {
								mySt.executeBatch();
							}
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					
				}
				try {
					mySt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("load data for directors table successfully!");
	}
	
	//load data for producers table
	public static void producers() {
		String producers="insert into Movie_Producer VALUES(?,?)";
		int i=0;
		boolean b1 = true;
		boolean b2 = true;
		try {
			mySt=myCon.prepareStatement(producers);
		} catch (SQLException e11) {
			// TODO Auto-generated catch block
			e11.printStackTrace();
		}
				InputStream gzipStream = null;
				try {
					gzipStream = new GZIPInputStream(new FileInputStream("/Users/penglitong/Desktop/title.principals.tsv.gz"));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Scanner sc=new Scanner(gzipStream,"UTF-8");
				sc.nextLine();
				Statement st1 = null;
				try {
					st1 = myCon.createStatement();
				} catch (SQLException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
				while(sc.hasNextLine()) {
					String[] scsplit = sc.nextLine().split("\t");
					String a = scsplit[2]; //nconst
					String b = scsplit[0]; //tconst
					String c = scsplit[3]; //category
					String sqla = "SELECT * FROM member WHERE id = '" + a+"'"; //producers should have Member id
					String sqlb = "SELECT * FROM movie WHERE id = '" + b+"'"; //Movie should have Movie id
					try {
						myRs = st1.executeQuery(sqla);
						b1 = myRs.next();
						myRs = st1.executeQuery(sqlb);
						b2 = myRs.next();
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					if (b1 == false || b2 == false) {
						continue;
					}
					if ("producer".equals(c)){
					
						try {
							mySt.setString(1, a);
							mySt.setString(2, b);
							mySt.addBatch();
							i++;
							if(i%10000 == 0 ) {
								mySt.executeBatch();
							}
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					
				}
				try {
					mySt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("load data for producers table successfully!");
	}
	
	//load data for genre of every movie
	public static void genretable() {
		Statement myS = null;
		try {
			myS=myCon.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String genretable="create table genretable"+
				"(id text,"+ //Movie id
				"genre text);"; //genre genre
		try {
						myS.executeUpdate(genretable);
						
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			myS.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("create genretable successfully!");
	}
	public static void loadGenretable() {
		int i=0;
		String loadGenretable="insert into genretable values(?,?)";
		try {
			mySt=myCon.prepareStatement(loadGenretable);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		InputStream gzipStream = null;
		try {
			gzipStream = new GZIPInputStream(new FileInputStream("/Users/penglitong/Desktop/title.basics.tsv.gz"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Scanner sc=new Scanner(gzipStream,"UTF-8");
		sc.nextLine();
		while(sc.hasNextLine()) {
			String[] scsplit = sc.nextLine().split("\t");
			String a=scsplit[0]; //Movie id
			String b=scsplit[8]; //genre genre
			try {
				mySt.setString(1, a);
				mySt.setString(2, b);
				mySt.addBatch();
				i++;
				if (i%100==0) {
					mySt.executeBatch();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			mySt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("load data for genretable table successfully!");
	}
	//load data for Genre_Movie table
	public static void loadMovieGenre() {
		String loadMovieGenre="insert into Movie_Genre select a.id, b.id"
				+ " from Genre a left join genretable b on a.genre=b.genre";
		try {
			mySt = myCon.prepareStatement(loadMovieGenre);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			mySt.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("loadMovieGenre done!");
	}
	
	//load data for Movie id, Member id, Role role
	public static void principals() {
		Statement myS = null;
		try {
			myS=myCon.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String principals="create table principals"+
				"(tconst text,"+ //Movie id
				"nconst text,"+ //Member id
				"characters text);"; //Role role
		try {
						myS.executeUpdate(principals);
						
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			myS.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("create principals successfully!");
	}
	public static void loadPrincipals() {
		int i=0;
		String load1="insert into principals VALUES(?,?,?)";
		try {
			mySt=myCon.prepareStatement(load1);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
				InputStream gzipStream = null;
				try {
					gzipStream = new GZIPInputStream(new FileInputStream("/Users/penglitong/Desktop/title.principals.tsv.gz"));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Scanner sc=new Scanner(gzipStream,"UTF-8");
				sc.nextLine();
				while(sc.hasNextLine()) {
					String[] scsplit = sc.nextLine().split("\t");
					String a = scsplit[0]; //Movie id
					String b = scsplit[2]; //Member id
					String c = scsplit[5]; //Role role
					try {
						mySt.setString(1, a);
						mySt.setString(2, b);
						mySt.setString(3, c);
						mySt.addBatch();
						i++;
						if(i%100 == 0 ) {
							mySt.executeBatch();
						}
						
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					
				}
				try {
					mySt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("load data for principals table successfully!");
	}
	
	//filter distinct actor and movie
	public static void pr() {
		Statement myS = null;
		try {
			myS=myCon.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String pr="create table pr"+
				"(tconst text,"+ //Movie id
				"nconst text);"; //Role role
		try {
						myS.executeUpdate(pr);
						
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			myS.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("create pr successfully!");
	}
	public static void prload() {
		myS = null;
		try {
			myS=myCon.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String prload="insert into pr select distinct tconst,nconst from principals;";
		try {
		
			myS.executeUpdate(prload);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			myS.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("load pr done!");
	}
	//load data for part of Actor_Movie_Role table(distinct actor and movie) with characters
		public static void pr1() {
			Statement myS = null;
			try {
				myS=myCon.createStatement();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String pr1="create table pr1"+
					"(tconst text,"+ //Movie id
					"nconst text,"+
					" characters text);"; //Role role
			try {
							myS.executeUpdate(pr1);
							
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				myS.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("create pr1 successfully!");
		}
		public static void pr1load() {
			myS = null;
			try {
				myS=myCon.createStatement();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String pr1load="insert into pr1 select a.tconst, a.nconst, b.characters"
					+ " from pr a left join principals b on a.tconst=b.tconst and a.nconst=b.nconst;";
			try {
			
				myS.executeUpdate(pr1load);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				myS.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("load pr1 done!");
		}


	//load data for role table
		public static void loadRole() {
			Statement myS = null;
			try {
				myS=myCon.createStatement();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String role="insert into role(role) select distinct characters from principals;"; //Role role
			try {
							myS.executeUpdate(role);
							
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				myS.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("load data for role table successfully!");
		}
	//load data for Actor_Movie_Role table
	public static void loadActor_Movie_Role() {
		myS = null;
		try {
			myS=myCon.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//disable the foreign key constraint for Actor_Movie_Role table
		String notNull="alter table Actor_Movie_Role drop constraint actor_movie_role_role_fkey;";
		//load data
		String loadMovieGenre="insert into Actor_Movie_Role select a.nconst, a.tconst, b.id"
				+ " from pr1 a left join Role b on a.characters=b.role;";
		//add the foreign key constraint for Actor_Movie_Role table
		String Null="alter table Actor_Movie_Role add constraint actor_movie_role_role_fkey foreign key (role) references Role(id);";
		try {
			myS.executeUpdate(notNull);
			myS.executeUpdate(loadMovieGenre);
			myS.executeUpdate(Null);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			myS.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("loadActor_Movie_Role done!");
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long starTime=System.currentTimeMillis();
		Load.connect();
		Load.table();
//		Load.loadRole();
//		Load.loadGenre();
//		Load.loadMember();
//		Load.tableMovie();
//		Load.load1();
//		Load.load2();
//		Load.load3();
//		Load.table2();
//		Load.combine1();
//		Load.combine2();
//		Load.actors();
//		Load.writers();
//		Load.directors();
//		Load.producers();
//		Load.genretable();
//		Load.loadGenretable();
//		Load.loadMovieGenre();
//		Load.principals();
//		Load.loadPrincipals();
//		Load.pr();
//		Load.prload();
//		Load.pr1();
//		Load.pr1load();
		Load.loadActor_Movie_Role();
		long endTime=System.currentTimeMillis();
		System.out.println("run"+(endTime-starTime)+"ms");
	}
}
