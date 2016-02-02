/**
 * 
 */
package com.mychaelstyle.common.datastore;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.mychaelstyle.common.GenericDatastore;
import com.mychaelstyle.common.GenericDatastore.ConfigurationException;
import com.mychaelstyle.common.GenericDatastore.ConnectionException;
import com.mychaelstyle.common.GenericDatastore.OperationException;
import com.mychaelstyle.common.GenericDatastore.Provider;
import com.mychaelstyle.common.GenericDatastore.ResultSet;

/**
 * MySQLに結果を出力する
 * @author Masanori Nakashima
 */
public class MySQL extends SQLDatabase {

    /**
     * コネクションマップ
     */
    private static Map<String,Connection> connMap = new HashMap<String,Connection>();

    /**
     * MySQL Provider
     */
    public MySQL() {
    }

    @Override
    public Connection getConnection() throws ConfigurationException {
        try {
            return getConnection(this.host, this.port, this.database, this.user, this.password);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            throw new ConfigurationException(e);
        }
    }

    /**
     * get database connection
     * @param host
     * @param databaseName
     * @param user
     * @param pw
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static Connection getConnection(String host, String port, String databaseName,
            String user, String pw) throws ClassNotFoundException, SQLException {
        String key = host+"-"+port+"-"+databaseName+"-"+user;
        synchronized(connMap){
            if(connMap.containsKey(key)){
                return connMap.get(key);
            }
        }
        String driverName   = "org.gjt.mm.mysql.Driver";
        String url = "jdbc:mysql://" + host+":"+ port +"/" + databaseName
                + "?useUnicode=true";
        Class.forName(driverName);
        Connection conn = DriverManager.getConnection(url, user, pw);
        synchronized(connMap){
            connMap.put(key, conn);
        }
        return conn;
    }

}
