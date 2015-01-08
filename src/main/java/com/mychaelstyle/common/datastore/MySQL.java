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
public class MySQL extends GenericDatastore.BaseProvider implements GenericDatastore.Provider{

    /**
     * コネクションマップ
     */
    private static Map<String,Connection> connMap = new HashMap<String,Connection>();

    /**
     * 
     */
    public MySQL() {
    }

    /**
     * 
     * @param host
     * @param databaseName
     * @param user
     * @param pw
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static Connection getConnection(String host, String databaseName,
            String user, String pw) throws ClassNotFoundException, SQLException {
        String key = host+"-"+databaseName+"-"+user;
        synchronized(connMap){
            if(connMap.containsKey(key)){
                return connMap.get(key);
            }
        }
        String driverName   = "org.gjt.mm.mysql.Driver";
        String url = "jdbc:mysql://" + host+ "/" + databaseName
                + "?useUnicode=true";
        Class.forName(driverName);
        Connection conn = DriverManager.getConnection(url, user, pw);
        synchronized(connMap){
            connMap.put(key, conn);
        }
        return conn;
    }

    @Override
    public Provider connect(JSONObject config) throws ConfigurationException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public JSONObject get() throws ConnectionException, ConfigurationException,
            OperationException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void delete() throws ConnectionException, ConfigurationException,
            OperationException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void put(JSONObject record) throws ConnectionException,
            ConfigurationException, OperationException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void update(JSONObject record) throws ConnectionException,
            ConfigurationException, OperationException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public JSONObject batchGet(JSONArray conditions)
            throws ConnectionException, ConfigurationException,
            OperationException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void batchWrite(JSONArray jsonArray) throws ConnectionException,
            ConfigurationException, OperationException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public ResultSet scan(JSONObject conditions, List<String> fields)
            throws ConnectionException, ConfigurationException,
            OperationException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultSet query(JSONObject conditions, List<String> fields)
            throws ConnectionException, ConfigurationException,
            OperationException {
        // TODO Auto-generated method stub
        return null;
    }

}
