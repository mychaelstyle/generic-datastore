/**
 * 
 */
package com.mychaelstyle.common.datastore;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

import com.mychaelstyle.common.GenericDatastore;
import com.mychaelstyle.common.GenericDatastore.ConfigurationException;
import com.mychaelstyle.common.GenericDatastore.ConnectionException;
import com.mychaelstyle.common.GenericDatastore.OperationException;
import com.mychaelstyle.common.GenericDatastore.Provider;

/**
 * Generic Abstract SQL RDBMS Provider
 * 
 * @author Masanori Nakashima
 *
 */
public abstract class SQLDatabase extends GenericDatastore.BaseProvider implements GenericDatastore.Provider {

    public static final String JSON_ITEM_ACTION = "action";
    public static final String JSON_VALUE_DELETE = "delete";
    public static final String JSON_ITEM_DATA = "data";
    public static final String JSON_ITEM_KEY = "key";
    public static final String JSON_ITEM_SUBKEY = "subkey";
    public static final String JSON_ITEM_TABLE_NAME = "table_name";
    
    public static final String JSON_ITEM_DATABASE_HOST = "database_host";
    public static final String JSON_ITEM_DATABASE_NAME = "database_name";
    public static final String JSON_ITEM_DATABASE_USER = "database_user";
    public static final String JSON_ITEM_DATABASE_PASSWORD = "database_password";

    protected JSONObject config = null;
    protected Connection connection = null;
    protected String host = "localhost";
    protected String database = "";
    protected String user = "";
    protected String password = "";

    /**
     * Constructor
     */
    public SQLDatabase() {
        super();
    }

    
    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#connect(org.json.JSONObject)
     */
    @Override
    public Provider connect(JSONObject config) throws ConfigurationException {
        this.config = config;
        this.host = config.getString(JSON_ITEM_DATABASE_HOST);
        this.database = config.getString(JSON_ITEM_DATABASE_NAME);
        this.user = config.getString(JSON_ITEM_DATABASE_USER);
        this.password = config.getString(JSON_ITEM_DATABASE_PASSWORD);
        this.connection = this.getConnection();
        return this;
    }

    /**
     * RDBMS接続を取得
     * @return
     * @throws ConfigurationException
     */
    public abstract Connection getConnection() throws ConfigurationException;

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#get()
     */
    @Override
    public JSONObject get() throws ConnectionException, ConfigurationException,
            OperationException {
        if(null==this.keyName || 0==this.keyName.length()){
            throw new OperationException("Primary key field name is not set yet!");
        } else if(null==this.keyValue){
            throw new OperationException("Primary key field value is not set yet!");
        }
        StringBuffer whereClauses = new StringBuffer()
        .append("`").append(this.keyName).append("`=?");
        if(null!=this.subkeyName && 0<this.subkeyName.length() && null!=this.subkeyValue){
            whereClauses.append(" AND `").append(this.subkeyName).append("`=?");
        }
        String sql = "SELECT * FROM `"+this.tableName+"` WHERE "+whereClauses.toString();

        PreparedStatement stmt = null;
        ResultSet resultSet = null;
        try {
            stmt = this.connection.prepareStatement(sql);
            int counter = 1;
            if(this.keyValue instanceof Integer){
                stmt.setInt(counter, (Integer)this.keyValue);
            } else if(this.keyValue instanceof Double){
                stmt.setDouble(counter, (Double)this.keyValue);
            } else {
                stmt.setString(counter, this.keyValue.toString());
            }
            counter++;
            if(null!=subkeyName){
                if(this.subkeyValue instanceof Integer){
                    stmt.setInt(counter, (Integer)this.subkeyValue);
                } else if(this.subkeyValue instanceof Double){
                    stmt.setDouble(counter, (Double)this.subkeyValue);
                } else {
                    stmt.setString(counter, this.subkeyValue.toString());
                }
            }
            resultSet = stmt.executeQuery();
            ResultSetMetaData rsmd = resultSet.getMetaData();
            List<String> fieldNames = new ArrayList<String>();
            for (int num = 1; num <= rsmd.getColumnCount(); num++) {
                fieldNames.add(rsmd.getColumnName(num));
            }
            JSONObject retData = new JSONObject();
            if(resultSet.next()){
                for(String fieldName:fieldNames){
                    Object val = resultSet.getObject(fieldName);
                    if(null==val){
                        retData.put(fieldName, "");
                    } else if(val instanceof Integer){
                        retData.put(fieldName, (Integer)val);
                    } else if(val instanceof Double){
                        retData.put(fieldName, (Double)val);
                    } else if(val instanceof Float){
                        retData.put(fieldName, (Float)val);
                    } else if(val instanceof java.sql.Date){
                        retData.put(fieldName, (Date)val);
                    } else {
                        retData.put(fieldName, val.toString());
                    }
                }
                return retData;
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new OperationException(e);
        } finally {
            this.keyName = null;
            this.keyValue = null;
            this.subkeyName = null;
            this.subkeyValue = null;
            this.tableName = null;
            try {
                if(null!=resultSet){
                    resultSet.close();
                }
                if(null!=stmt){
                    stmt.close();
                }
            } catch (SQLException e) {
                throw new OperationException(e);
            }
        }

    }

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#delete()
     */
    @Override
    public void delete() throws ConnectionException, ConfigurationException,
            OperationException {
        if(null==this.keyName || 0==this.keyName.length()){
            throw new OperationException("Primary key field name is not set yet!");
        } else if(null==this.keyValue){
            throw new OperationException("Primary key field value is not set yet!");
        }
        StringBuffer whereClauses = new StringBuffer()
        .append("`").append(this.keyName).append("`=?");
        if(null!=this.subkeyName && 0<this.subkeyName.length() && null!=this.subkeyValue){
            whereClauses.append(" AND `").append(this.subkeyName).append("`=?");
        }
        String sql = "DELETE FROM `"+this.tableName+"` WHERE "+whereClauses.toString();

        PreparedStatement stmt = null;
        try {
            stmt = this.connection.prepareStatement(sql);
            int counter = 1;
            if(this.keyValue instanceof Integer){
                stmt.setInt(counter, (Integer)this.keyValue);
            } else if(this.keyValue instanceof Double){
                stmt.setDouble(counter, (Double)this.keyValue);
            } else {
                stmt.setString(counter, this.keyValue.toString());
            }
            counter++;
            if(null!=subkeyName){
                if(this.subkeyValue instanceof Integer){
                    stmt.setInt(counter, (Integer)this.subkeyValue);
                } else if(this.subkeyValue instanceof Double){
                    stmt.setDouble(counter, (Double)this.subkeyValue);
                } else {
                    stmt.setString(counter, this.subkeyValue.toString());
                }
            }
            stmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new OperationException(e);
        } finally {
            this.keyName = null;
            this.keyValue = null;
            this.subkeyName = null;
            this.subkeyValue = null;
            this.tableName = null;
            try {
                if(null!=stmt){
                    stmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw new OperationException(e);
            }
        }
    }

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#put(org.json.JSONObject)
     */
    @Override
    public void put(JSONObject record) throws ConnectionException,
            ConfigurationException, OperationException {
        JSONObject row = this.withTable(this.tableName)
        .withKey(this.keyName, this.keyValue)
        .withSubkey(this.subkeyName, this.subkeyValue).get();
        if(null==row){
            this.insert(record);
        } else {
            this.update(record);
        }
        this.keyName = null;
        this.keyValue = null;
        this.subkeyName = null;
        this.subkeyValue = null;
        this.tableName = null;
    }

    /**
     * update
     * @param data
     * @throws ConnectionException
     * @throws ConfigurationException
     * @throws OperationException
     */
    public void update(JSONObject data)
            throws ConnectionException, ConfigurationException, OperationException {
        StringBuffer updatesStr = new StringBuffer();
        @SuppressWarnings("unchecked")
        Set<String> keys = (Set<String>) data.keySet();
        List<String> fields = new ArrayList<String>();
        for(String key:keys){
            if(key.equalsIgnoreCase(this.keyName)
                    || (null!=this.subkeyName && key.equalsIgnoreCase(this.subkeyName))){
                continue;
            }
            if(updatesStr.length()>0){
                updatesStr.append(",");
            }
            updatesStr.append("`").append(key).append("`=?");
            fields.add(key);
        }
        StringBuffer whereClauses = new StringBuffer();
        whereClauses.append("`").append(this.keyName).append("`=?");
        if(null!=this.subkeyName){
            whereClauses.append(" AND ");
            whereClauses.append("`").append(this.subkeyName).append("`=?");
        }
        String sql = "UPDATE "+this.tableName+" SET "+updatesStr.toString()
                +" WHERE "+whereClauses.toString();
        Connection conn = this.getConnection();
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(sql);
            int counter = 1;
            for(String key:fields){
                if(key.equalsIgnoreCase(this.keyName)
                        || (null!=subkeyName && key.equalsIgnoreCase(this.subkeyName))){
                    continue;
                }
                Object obj = data.get(key);
                if(obj instanceof Integer){
                    stmt.setInt(counter, (Integer) obj);
                } else if(obj instanceof Double){
                    stmt.setDouble(counter, (Double) obj);
                } else {
                    stmt.setString(counter, (String) obj);
                }
                counter++;
            }
            Object obj = data.get(this.keyName);
            if(obj instanceof Integer){
                stmt.setInt(counter, (Integer)obj);
            } else if(obj instanceof Double){
                stmt.setDouble(counter, (Double)obj);
            } else {
                stmt.setString(counter, obj.toString());
            }
            counter++;
            if(null!=this.subkeyName){
                obj = data.get(this.subkeyName);
                if(obj instanceof Integer){
                    stmt.setInt(counter, (Integer)obj);
                } else if(obj instanceof Double){
                    stmt.setDouble(counter, (Double)obj);
                } else {
                    stmt.setString(counter, obj.toString());
                }
            }
            stmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new OperationException(e);
        } finally {
            try {
                if(null!=stmt){
                    stmt.close();
                }
            } catch (SQLException e) {
                throw new OperationException(e);
            }
        }
    }

    /**
     * insert
     * @param data
     * @throws ConnectionException
     * @throws ConfigurationException
     * @throws OperationException
     */
    protected void insert(JSONObject data) throws ConnectionException, ConfigurationException, OperationException {
        List<String> fields = new ArrayList<String>();
        List<Object> values = new ArrayList<Object>();
        @SuppressWarnings("unchecked")
        Set<String> keys = (Set<String>) data.keySet();
        for(String key:keys){
            fields.add(key);
            Object obj = data.get(key);
            if(null==obj){
                values.add("");
            } if(obj instanceof Integer){
                Integer val = (Integer) obj;
                values.add(val);
            } else if(obj instanceof Double){
                Double val = (Double) obj;
                values.add(val);
            } else {
                values.add(obj.toString());
            }
        }
        StringBuffer fieldsStr = new StringBuffer();
        StringBuffer valueStr = new StringBuffer();
        for(String name:fields){
            if(fieldsStr.length()>0){
                fieldsStr.append(",");
            }
            if(valueStr.length()>0){
                valueStr.append(",");
            }
            fieldsStr.append("`").append(name).append("`");
            valueStr.append("?");
        }
        String sql = "INSERT INTO `"+this.tableName+"` ("+fieldsStr+") VALUES ("+valueStr+")";
        Connection conn = this.getConnection();
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(sql);
            int counter = 1;
            for(Object obj:values){
                if(obj instanceof Integer){
                    stmt.setInt(counter, (Integer) obj);
                } else if(obj instanceof Double){
                    stmt.setDouble(counter, (Double) obj);
                } else {
                    stmt.setString(counter, (String) obj);
                }
                counter++;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new OperationException(e);
        } finally {
            try {
                if(null!=stmt){
                    stmt.execute();
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw new OperationException(e);
            }
        }
    }

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#batchGet(org.json.JSONArray)
     */
    @Override
    public JSONObject batchGet(JSONArray conditions)
            throws ConnectionException, ConfigurationException,
            OperationException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#batchWrite(org.json.JSONArray)
     */
    @Override
    public void batchWrite(JSONArray jsonArray) throws ConnectionException,
            ConfigurationException, OperationException {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#scan(org.json.JSONObject, java.util.List)
     */
    @Override
    public com.mychaelstyle.common.GenericDatastore.ResultSet scan(
            JSONObject conditions, List<String> fields)
            throws ConnectionException, ConfigurationException,
            OperationException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#query(org.json.JSONObject, java.util.List)
     */
    @Override
    public com.mychaelstyle.common.GenericDatastore.ResultSet query(
            JSONObject conditions, List<String> fields)
            throws ConnectionException, ConfigurationException,
            OperationException {
        // TODO Auto-generated method stub
        return null;
    }

}
