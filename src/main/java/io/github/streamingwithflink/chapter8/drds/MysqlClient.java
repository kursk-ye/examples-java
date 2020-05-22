package io.github.streamingwithflink.chapter8.drds;

import io.github.streamingwithflink.chapter8.CommonPOJO;

import java.sql.*;

public class MysqlClient {
  private String url = "****";
  private String dbName = "***";
  private String driver = "com.mysql.cj.jdbc.Driver";
  /// password!!!!!!!!!!!!!!!!!!!!!!!!!!!
  private String userName = "***";
  private String password = "****";

  private Connection connect = null;
  private Statement statement = null;
  private PreparedStatement preparedStatement = null;
  ResultSet resultSet = null;

  public MysqlClient() throws SQLException, ClassNotFoundException, IllegalAccessException, InstantiationException {
    try {
      Class driver_class = Class.forName(driver);
      Driver driver = (Driver) driver_class.newInstance();
      DriverManager.registerDriver(driver);

    } catch (Exception e) {
      throw e;
    } finally {
      close();
    }
  }

  public <T extends CommonPOJO> void writeDB(T record) {
    try {
      // why in MysqlClient constructor method cause error
      // com.mysql.cj.exceptions.ConnectionIsClosedException: No operations allowed after connection closed
      connect = DriverManager.getConnection(url + dbName,userName,password);

      preparedStatement =
          connect.prepareStatement(
              "insert into  YL_TEST values (?, ?, ?)");
      preparedStatement.setString(1, record.getKey());
      preparedStatement.setTimestamp(2,  new Timestamp(System.currentTimeMillis()) );
      preparedStatement.setDouble(3, (Double) record.getValue()) ;

      preparedStatement.executeUpdate();

    } catch (SQLException throwables) {
      throwables.printStackTrace();
    }finally{
      close();
    }
  }

  public ResultSet readDB(){

    try{
      connect = DriverManager.getConnection(url + dbName,userName,password);
      preparedStatement = connect.prepareStatement("select id ,mid,dayelecvalue from YL_TEST limit 0 , 10");
      resultSet = preparedStatement.executeQuery();

    } catch (SQLException throwables) {
      throwables.printStackTrace();
    } finally{
      // not close here!
      // close();
    }
    return resultSet;
  }

  // You need to close the resultSet
  private void close() {
    try {
      if (resultSet != null) {
        resultSet.close();
      }

      if (statement != null) {
        statement.close();
      }

      if (connect != null) {
        connect.close();
      }
    } catch (Exception e) {

    }
  }
}
