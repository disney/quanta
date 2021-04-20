package com.disney.dmed.quanta.example;

import java.io.*;
import java.sql.*;

/**
 * A Java Quanta SELECT statement example.
 * Demonstrates the use of a SQL SELECT statement against
 * the Quanta connection proxy, called from a Java program.
 * 
 */
public class JavaQuantaSelectExample {

  public static void main(String[] args) {

    if (args.length != 3) {
        System.out.format("Usage: <host:port> <input_sql_file> <jwt_access_token>\n");
        System.exit(0);
    }

    try {
      String myDriver = "com.mysql.cj.jdbc.Driver";
      String myUrl = String.format("jdbc:mysql://%s/quanta?logger=com.mysql.cj.log.Slf4JLogger", args[0]);
      Class.forName(myDriver);
      Connection conn = DriverManager.getConnection(myUrl, args[2], "");

      File file = new File(args[1]);
      BufferedReader br = new BufferedReader(new FileReader(file));

      String query; 
      while ((query = br.readLine()) != null) {

        StringBuilder sb = new StringBuilder(query);
        int semi = sb.lastIndexOf(";");
        if (semi > 0) {
            sb.deleteCharAt(semi);
        }
        query = sb.toString();

        // Is it a count or export query?
        boolean isRownum = sb.lastIndexOf("@rownum") != -1;

        System.out.println(query); 
      
        Statement st = conn.createStatement();
        long start = System.currentTimeMillis();
        ResultSet rs = st.executeQuery(query);
        
        while (rs.next()) {
          int result = 0;
          if (isRownum)
              result = rs.getInt("@rownum");
          else 
              result = rs.getInt("count(*)");

          System.out.format("RESULT = %s\n", result);
        }
        st.close();
        System.out.format("elapsed time %d ms.\n\n", (System.currentTimeMillis() - start));
      } 
      conn.close();
    }
    catch (Exception e) {
      System.err.println("Got an exception! ");
      System.err.println(e);
    }
  }
}
