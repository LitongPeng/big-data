package assign1;


//Litong Peng
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

public class pro3 {
	
	
	
	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub
		long starTime=System.currentTimeMillis();
		//connect to the postgresql
		//
		Connection myCon=null;
		PreparedStatement mySt=null;
		ResultSet myRe=null;
		
		String url="jdbc:postgresql://127.0.0.1:5432/postgres";
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
		System.out.println("open successfully!");
	
		
//		create the non_adultMovies table
		Statement myCs1 = null;
		try {
			myCs1=myCon.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String sql1="create table non_adultMovies"+
		"(tconst integer,"+
		"genres varchar(255),"+
		"ratings real,"+
		"votes integer,"+
		"primary key(tconst));";
		try {
			myCs1.executeUpdate(sql1);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			myCs1.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("create the non_adultMovies table successfully!");
		
		
//		create the name table
				Statement myCs2 = null;
				try {
					myCs2=myCon.createStatement();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				String sql2="create table name"+
				"(nconst integer,"+
				"primaryName varchar(255),"+
				"birthYear integer,"+
				"deathYear integer,"+
				"primary key(nconst));";
				try {
					myCs2.executeUpdate(sql2);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					myCs2.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("create the name table successfully!");
				
				
				//create the actors table
				Statement myCs3 = null;
				try {
					myCs3=myCon.createStatement();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				String sql3="create table actors"+
				"(movie integer,"+
				"actor integer,"+
				"foreign key(movie) references non_adultMovies(tconst),"+
				"foreign key(actor) references name(nconst));";
				try {
					myCs3.executeUpdate(sql3);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					myCs3.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("create the actors table successfully!");
				
				
				//create the directors table
				Statement myCs4 = null;
				try {
					myCs4=myCon.createStatement();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				String sql4="create table directors"+
				"(movie integer,"+
				"director integer,"+
				"foreign key(movie) references non_adultMovies(tconst),"+
				"foreign key(director) references name(nconst));";
				try {
					myCs4.executeUpdate(sql4);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					myCs4.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("create the directors table successfully!");
				
				
				//create the writers table
				Statement myCs5 = null;
				try {
					myCs5=myCon.createStatement();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				String sql5="create table writers"+
				"(movie integer,"+
				"writer integer,"+
				"foreign key(movie) references non_adultMovies(tconst),"+
				"foreign key(writer) references name(nconst));";
				try {
					myCs5.executeUpdate(sql5);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					myCs5.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("create the writers table successfully!");
				
				
				//create the producers table
				Statement myCs6 = null;
				try {
					myCs6=myCon.createStatement();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				String sql6="create table producers"+
				"(movie integer,"+
				"producer integer,"+
				"foreign key(movie) references non_adultMovies(tconst),"+
				"foreign key(producer) references name(nconst));";
				try {
					myCs6.executeUpdate(sql6);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					myCs6.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("create the producers table successfully!");
				
				
				//load the data for directors table
				//read the gzip file on each line, split the line use \t
				//select the string and insert it to the table
		int i1 = 0;
		String sql7="insert into name VALUES(?,?,?,?)";
		try {
			mySt=myCon.prepareStatement(sql7);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
				InputStream gzipStream1 = null;
				try {
					gzipStream1 = new GZIPInputStream(new FileInputStream("/Users/penglitong/Desktop/name.basics.tsv.gz"));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Scanner sc1=new Scanner(gzipStream1,"UTF-8");
				sc1.nextLine();
				while(sc1.hasNextLine()) {
					String[] scsplit = sc1.nextLine().split("\t");
					String a = scsplit[0].substring(2);
					Integer aa = Integer.valueOf(a);
					String b = scsplit[1];
					String c = scsplit[2];
					String d = scsplit[3];
					
					try {
						mySt.setInt(1, aa);
						mySt.setString(2, b);
						
						if(!"\\N".equals(c)) {
							mySt.setInt(3, Integer.valueOf(c));
						}
						
						if(!"\\N".equals(d)) {
							mySt.setInt(4, Integer.valueOf(d));
						}
						
						mySt.addBatch();
						i1++;
						if(i1%1000 == 0 ) {
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
				System.out.println("load data for directors table successfully!");
				
				
				//load the data for movie
		//create genre table and ratingvote table
		Statement myCs7 = null;
		try {
			myCs7=myCon.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String sql8="create table genre"+
		"(tconst integer,"+
		"genres varchar(255),"+
		"primary key(tconst));";
		String sql9="create table ratingVote"+
				"(tconst integer,"+
				"rating real,"+
				"vote integer,"+
				"primary key(tconst));";
		try {
			myCs7.executeUpdate(sql8);
			myCs7.executeUpdate(sql9);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			myCs7.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//load the data for genre table
		int i2=0;
		String sql10="insert into genre VALUES(?,?)";
		try {
			mySt=myCon.prepareStatement(sql10);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
				InputStream gzipStream2 = null;
				try {
					gzipStream2 = new GZIPInputStream(new FileInputStream("/Users/penglitong/Desktop/title.basics.tsv.gz"));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Scanner sc2=new Scanner(gzipStream2,"UTF-8");
				sc2.nextLine();
				while(sc2.hasNextLine()) {
					String[] scsplit = sc2.nextLine().split("\t");
					String a = scsplit[0].substring(2);
					Integer aa = Integer.valueOf(a);
					String b = scsplit[8];
					try {
						mySt.setInt(1, aa);
						mySt.setString(2, b);
						mySt.addBatch();
						i2++;
						if(i2%1000 == 0 ) {
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
				System.out.println("load data for genre table successfully!");
				
				
				//load the data for ratingvote table
		int i3=0;
		String sql11="insert into ratingVote VALUES(?,?,?)";
		try {
			mySt=myCon.prepareStatement(sql11);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
				InputStream gzipStream3 = null;
				try {
					gzipStream3 = new GZIPInputStream(new FileInputStream("/Users/penglitong/Desktop/title.ratings.tsv.gz"));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Scanner sc3=new Scanner(gzipStream3,"UTF-8");
				sc3.nextLine();
				while(sc3.hasNextLine()) {
					String[] scsplit = sc3.nextLine().split("\t");
					String a = scsplit[0].substring(2);
					Integer aa = Integer.valueOf(a);
					String b = scsplit[1];
					String c = scsplit[2];
					Integer cc = Integer.valueOf(c);
					try {
						mySt.setInt(1, aa);
						mySt.setFloat(2, Float.valueOf(b));
						mySt.setInt(3, cc);
						mySt.addBatch();
						i3++;
						if(i3%1000 == 0 ) {
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
				System.out.println("load data for ratingvote table successfully!");
		
		//combine the genre table and ratingvote table
				//if the tconst from general table and ratingvote table are same then combine
		String sql12="insert into non_adultMovies select a.tconst, a.genres, b.rating, b.vote "
				+ "from genre a left join ratingVote b on a.tconst=b.tconst";
		try {
			mySt = myCon.prepareStatement(sql12);
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
		
		//load data for actors table
		//when the category is actor 
		//and if the matched nconst can be found from name table 
		//and the matched tconst can be found from the non_adultMovies table
		//then insert this nconst and tconst into the actors table
	
		int i4=0;
		String sql13="insert into actors VALUES(?,?)";
		boolean e1a;
		boolean e2a;
		try {
			mySt=myCon.prepareStatement(sql13);
		} catch (SQLException e11) {
			// TODO Auto-generated catch block
			e11.printStackTrace();
		}
				InputStream gzipStream4 = null;
				try {
					gzipStream4 = new GZIPInputStream(new FileInputStream("/Users/penglitong/Desktop/title.principals.tsv.gz"));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Scanner sc4=new Scanner(gzipStream4,"UTF-8");
				sc4.nextLine();
				Statement st1 = myCon.createStatement();
				while(sc4.hasNextLine()) {
					String[] scsplit = sc4.nextLine().split("\t");
					String a = scsplit[0].substring(2);
					Integer aa = Integer.valueOf(a);
					String b = scsplit[2].substring(2);
					Integer bb = Integer.valueOf(b);
					String c = scsplit[3];
					String sqla = "SELECT * FROM name WHERE nconst = " + bb;
					String sqlb = "SELECT * FROM non_adultMovies WHERE tconst = " + aa;
					myRe = st1.executeQuery(sqla);
					e1a = myRe.next();
					myRe = st1.executeQuery(sqlb);
					e2a = myRe.next();
					if (e1a == false || e2a == false) {
						continue;
					}
					if ("actor".equals(c)){
					
						try {
							mySt.setInt(1, aa);
							mySt.setInt(2, bb);
							mySt.addBatch();
							i4++;
							if(i4%1000 == 0 ) {
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
				
				
				//load data for directors table
				int i5=0;
				String sql14="insert into directors VALUES(?,?)";
				boolean e1b;
				boolean e2b;
				try {
					mySt=myCon.prepareStatement(sql14);
				} catch (SQLException e11) {
					// TODO Auto-generated catch block
					e11.printStackTrace();
				}
						InputStream gzipStream5 = null;
						try {
							gzipStream5 = new GZIPInputStream(new FileInputStream("/Users/penglitong/Desktop/title.principals.tsv.gz"));
						} catch (FileNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						Scanner sc5=new Scanner(gzipStream5,"UTF-8");
						sc5.nextLine();
						Statement st2 = myCon.createStatement();
						while(sc5.hasNextLine()) {
							String[] scsplit = sc5.nextLine().split("\t");
							String a = scsplit[0].substring(2);
							Integer aa = Integer.valueOf(a);
							String b = scsplit[2].substring(2);
							Integer bb = Integer.valueOf(b);
							String c = scsplit[3];
							String sqlc = "SELECT * FROM name WHERE nconst = " + bb;
							String sqld = "SELECT * FROM non_adultMovies WHERE tconst = " + aa;
							myRe = st2.executeQuery(sqlc);
							e1b = myRe.next();
							myRe = st2.executeQuery(sqld);
							e2b = myRe.next();
							if (e1b == false || e2b == false) {
								continue;
							}
							if ("director".equals(c)){
							
								try {
									mySt.setInt(1, aa);
									mySt.setInt(2, bb);
									mySt.addBatch();
									i5++;
									if(i5%1000 == 0 ) {
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
						
						
						//load data for writers table
						int i6=0;
						String sql15="insert into writers VALUES(?,?)";
						boolean e1c;
						boolean e2c;
						try {
							mySt=myCon.prepareStatement(sql15);
						} catch (SQLException e11) {
							// TODO Auto-generated catch block
							e11.printStackTrace();
						}
								InputStream gzipStream6 = null;
								try {
									gzipStream6 = new GZIPInputStream(new FileInputStream("/Users/penglitong/Desktop/title.principals.tsv.gz"));
								} catch (FileNotFoundException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								Scanner sc6=new Scanner(gzipStream6,"UTF-8");
								sc6.nextLine();
								Statement st3 = myCon.createStatement();
								while(sc6.hasNextLine()) {
									String[] scsplit = sc6.nextLine().split("\t");
									String a = scsplit[0].substring(2);
									Integer aa = Integer.valueOf(a);
									String b = scsplit[2].substring(2);
									Integer bb = Integer.valueOf(b);
									String c = scsplit[3];
									String sqle = "SELECT * FROM name WHERE nconst = " + bb;
									String sqlf = "SELECT * FROM non_adultMovies WHERE tconst = " + aa;
									myRe = st3.executeQuery(sqle);
									e1c = myRe.next();
									myRe = st3.executeQuery(sqlf);
									e2c = myRe.next();
									if (e1c == false || e2c == false) {
										continue;
									}
									if ("writer".equals(c)){
									
										try {
											mySt.setInt(1, aa);
											mySt.setInt(2, bb);
											mySt.addBatch();
											i6++;
											if(i6%1000 == 0 ) {
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
								
								
								//load data for writers table
								int i7=0;
								String sql16="insert into producers VALUES(?,?)";
								boolean e1d;
								boolean e2d;
								try {
									mySt=myCon.prepareStatement(sql16);
								} catch (SQLException e11) {
									// TODO Auto-generated catch block
									e11.printStackTrace();
								}
										InputStream gzipStream7 = null;
										try {
											gzipStream7 = new GZIPInputStream(new FileInputStream("/Users/penglitong/Desktop/title.principals.tsv.gz"));
										} catch (FileNotFoundException e) {
											// TODO Auto-generated catch block
											e.printStackTrace();
										} catch (IOException e) {
											// TODO Auto-generated catch block
											e.printStackTrace();
										}
										Scanner sc7=new Scanner(gzipStream7,"UTF-8");
										sc7.nextLine();
										Statement st4 = myCon.createStatement();
										while(sc7.hasNextLine()) {
											String[] scsplit = sc7.nextLine().split("\t");
											String a = scsplit[0].substring(2);
											Integer aa = Integer.valueOf(a);
											String b = scsplit[2].substring(2);
											Integer bb = Integer.valueOf(b);
											String c = scsplit[3];
											String sqlg = "SELECT * FROM name WHERE nconst = " + bb;
											String sqlh = "SELECT * FROM non_adultMovies WHERE tconst = " + aa;
											myRe = st4.executeQuery(sqlg);
											e1d = myRe.next();
											myRe = st4.executeQuery(sqlh);
											e2d = myRe.next();
											if (e1d == false || e2d == false) {
												continue;
											}
											if ("producer".equals(c)){
											
												try {
													mySt.setInt(1, aa);
													mySt.setInt(2, bb);
													mySt.addBatch();
													i7++;
													if(i7%1000 == 0 ) {
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
										
										
										//print run time
										//I load the whole database in approximately 4 hours
										long endTime=System.currentTimeMillis();
										System.out.println("run"+(endTime-starTime)+"ms");
										
										
										//tansaction
										//the first and third row is true,but the second row is error
										//so all of them will not execute
										try {
										myCon.setAutoCommit(false);
										Statement stm = myCon.createStatement();
										stm.executeUpdate("insert into actors(movie, actor) values (3377,4499)");
										stm.executeUpdate("insert into actors(movie, actor) values ('cc')");
										stm.executeUpdate("insert into actors(movie, actor) values (3477,4599)");
										myCon.commit();
										}catch(SQLException e) {
											
												myCon.rollback();
											
										}
										


	}

}
