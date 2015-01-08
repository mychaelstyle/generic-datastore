/**
 * 
 */
package com.mychaelstyle.common.datastore;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.mychaelstyle.common.GenericDatastore;
import com.mychaelstyle.common.GenericDatastore.BaseProvider;
import com.mychaelstyle.common.GenericDatastore.ConfigurationException;
import com.mychaelstyle.common.GenericDatastore.ConnectionException;
import com.mychaelstyle.common.GenericDatastore.OperationException;
import com.mychaelstyle.common.GenericDatastore.Provider;
import com.mychaelstyle.common.GenericDatastore.ResultSet;

/**
 * GenericDatastoreが利用するデータストアプロバイダのRedis実装です.
 * 
 * 内部的に「テーブル名::主キー値::副キー値」のように「::」で連結した文字列をキーとしてRedisに登録します。<br>
 * このため値に::が含まれるような設計のテーブルは作成できませんので注意してください。<br>
 * <br>
 * scanとqueryは同じ実装です.<br>
 * 
 * <br>
 * configのフォーマット<br>
 * <pre>
 * {
 *     "host" : "host name",
 *     "port" : "port number",
 *     "slaves" : [
 *     ]
 * }
 * </pre>
 * 
 * @author Masanori Nakashima
 */
public class Redis extends BaseProvider {

    /** 設定項目 : ホスト名 */
    public static final String CONFIG_HOST = "host";
    /** 設定項目 : ポート番号 */
    public static final String CONFIG_PORT = "port";
    /** 設定項目 : スレーブ */
    public static final String CONFIG_SLAVES = "slaves";
    /** キー連結文字列 */
    public static final String KEY_DELIMITER = "::";

    /**
     * Jedisインスタンスプール
     */
    private static Map<String,JedisPool> poolMap = new HashMap<String,JedisPool>();

    /**
     * configuration JSON object
     */
    private JSONObject config = null;

    /** Jedisクライアントインスタンス */
    private Jedis jedis = null;

    /**
     * constructor
     */
    public Redis() {
        super();
    }

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#connect(org.json.JSONObject)
     */
    @Override
    public Provider connect(final JSONObject config) throws ConfigurationException {
        this.config = config;
        String host = this.config.getString(CONFIG_HOST);
        int port = this.config.getInt(CONFIG_PORT);
        String name = this.getPoolKey();
        synchronized(poolMap){
            if(!poolMap.containsKey(name)){
                JedisPool pool = new JedisPool(new JedisPoolConfig(), host, port);
                poolMap.put(name, pool);
            }
        }

        JedisPool pool = poolMap.get(name);
        this.jedis = pool.getResource();
        if(config.has(CONFIG_SLAVES)){
            JSONArray slavesArray = config.getJSONArray(CONFIG_SLAVES);
            for(int num=0; num<slavesArray.length(); num++){
                JSONObject slave = slavesArray.getJSONObject(num);
                String shost = slave.getString(CONFIG_HOST);
                int sport = slave.getInt(CONFIG_PORT);
                this.jedis.slaveof(shost, sport);
            }
        }
        return this;
    }

    private String getPoolKey(){
        String host = this.config.getString(CONFIG_HOST);
        int port = this.config.getInt(CONFIG_PORT);
        return host+":"+port;
    }

    private static String createKeyString(String table,String keyValue, String subkeyValue){
        return table+KEY_DELIMITER+keyValue
                +((null==subkeyValue || subkeyValue.length()==0) ? "" : KEY_DELIMITER+subkeyValue);
    }

    private String getQueryKey(){
        return createKeyString(this.tableName,this.keyValue,this.subkeyValue);
    }

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#get()
     */
    @Override
    public JSONObject get() throws ConnectionException, ConfigurationException,
            OperationException {
        String value = this.jedis.get(this.getQueryKey());
        if(null!=value && value.length()>0){
            return new JSONObject(value);
        }
        return null;
    }

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#delete()
     */
    @Override
    public void delete() throws ConnectionException, ConfigurationException,
            OperationException {
        this.jedis.del(this.getQueryKey());

    }

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#put(org.json.JSONObject)
     */
    @Override
    public void put(JSONObject record) throws ConnectionException,
            ConfigurationException, OperationException {
        if(!record.has(this.keyName)){
            record.put(this.keyName, this.keyValue);
        }
        if(null!=this.subkeyName && !record.has(this.subkeyName)){
            record.put(this.subkeyName, this.subkeyValue);
        }
        this.jedis.set(this.getQueryKey(), record.toString());
    }

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#update(org.json.JSONObject)
     */
    @Override
    public void update(JSONObject record) throws ConnectionException,
            ConfigurationException, OperationException {
        JSONObject org = this.get();
        @SuppressWarnings("unchecked")
        Set<String> keys = record.keySet();
        for(String key: keys){
            org.put(key, record.get(key));
        }
        this.put(org);
    }

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#batchGet(org.json.JSONArray)
     */
    @Override
    public JSONObject batchGet(JSONArray conditions)
            throws ConnectionException, ConfigurationException,
            OperationException {
        JSONObject result = new JSONObject();
        for(int num=0; num<conditions.length(); num++){
            JSONObject condition = conditions.getJSONObject(num);
            String table = condition.getString(FIELD_TABLE);
            String keyName = condition.getString(FIELD_KEY);
            String subkeyName = null;
            if(condition.has(FIELD_SUBKEY)){
                subkeyName = condition.getString(FIELD_SUBKEY);
            }
            JSONObject cond = condition.getJSONObject(FIELD_DATA);
            String keyVal = cond.getString(keyName);
            String subkeyVal = null;
            if(null!=subkeyName && cond.has(subkeyName)){
                subkeyVal = cond.getString(subkeyName);
            }
            JSONObject row = this.withTable(table).withKey(keyName, keyVal)
                    .withSubkey(subkeyName, subkeyVal).get();
            if(null!=row){
                JSONArray records = new JSONArray();
                if(result.has(table)){
                    records = result.getJSONArray(table);
                }
                records.put(row);
                result.put(table, records);
            }
        }
        return result;
    }

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#batchWrite(org.json.JSONArray)
     */
    @Override
    public void batchWrite(JSONArray jsonArray) throws ConnectionException,
            ConfigurationException, OperationException {
        for(int num=0; num<jsonArray.length(); num++){
            JSONObject condition = jsonArray.getJSONObject(num);
            String table = condition.getString(FIELD_TABLE);
            String keyName = condition.getString(FIELD_KEY);
            String subkeyName = null;
            if(condition.has(FIELD_SUBKEY)){
                subkeyName = condition.getString(FIELD_SUBKEY);
            }
            String action = condition.getString(FIELD_ACTION);
            JSONObject data = condition.getJSONObject(FIELD_DATA);
            String keyVal = data.getString(keyName);
            String subkeyVal = null;
            if(null!=subkeyName && subkeyName.length()>0){
                subkeyVal = data.getString(subkeyName);
            }
            if(ACTION_DELETE.equalsIgnoreCase(action)){
                // delete
                this.withTable(table).withKey(keyName, keyVal)
                .withSubkey(subkeyName, subkeyVal).delete();
            } else if(ACTION_PUT.equalsIgnoreCase(action)){
                // put
                this.withTable(table).withKey(keyName, keyVal)
                .withSubkey(subkeyName, subkeyVal).put(data);
            }
        }
    }

    /**
     * スキャンやクエリーの条件に応じたRedisのキー問い合わせ文字列を生成.
     * TODO : 現状一度に全てのキーを取得する実装。
     * 結果が膨大になる場合の対策が必要。ワイルドカードの前の文字を指定して結果を分割できるようにするなど。
     * @param conditions
     * @return
     */
    private String createScanKeyString(JSONObject conditions){
        StringBuffer buf = new StringBuffer(this.tableName).append(KEY_DELIMITER);
        if(null!=conditions){
            if(conditions.has(this.keyName)){
                JSONObject cond = conditions.getJSONObject(this.keyName);
                String keyVal = cond.getString(GenericDatastore.Provider.FIELD_VALUE);
                buf.append(keyVal);
            } else {
                buf.append("*");
            }
            String subkeyVal = null;
            if(null!=this.subkeyName && this.subkeyName.length()>0){
                JSONObject cond = conditions.getJSONObject(this.subkeyName);
                subkeyVal = cond.getString(GenericDatastore.Provider.FIELD_VALUE);
            }
            if(null!=subkeyVal){
                buf.append(KEY_DELIMITER).append(subkeyVal);
            }
        }
        buf.append("*");
        return buf.toString();
    }

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#scan(org.json.JSONObject, java.util.List, int)
     */
    @Override
    public ResultSet scan(JSONObject conditions, List<String> fields)
            throws ConnectionException, ConfigurationException,
            OperationException {
        String str = this.createScanKeyString(conditions);
        return new RedisResultSet(this.jedis,str);
    }

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#query(org.json.JSONObject, java.util.List, int)
     */
    @Override
    public ResultSet query(JSONObject conditions, List<String> fields)
            throws ConnectionException, ConfigurationException,
            OperationException {
        String str = this.createScanKeyString(conditions);
        return new RedisResultSet(this.jedis,str);
    }

    /**
     * Redisスキャン、クエリに対するGenericDatastore.ResultSetの実装.
     * 
     * @author Masanori Nakashima
     */
    public static class RedisResultSet implements GenericDatastore.ResultSet {
        /** Jedis instance */
        private Jedis jedis = null;
        /** keys iterator */
        private Iterator<String> keysIterator = null;
        /**
         * Constructor
         * @param jedis
         * @param scanPrefix
         */
        protected RedisResultSet(Jedis jedis, String scanPrefix){
            this.jedis = jedis;
            Set<String> keys = jedis.keys(scanPrefix);
            this.keysIterator = keys.iterator();
        }

        @Override
        public JSONObject next() {
            String key = this.keysIterator.next();
            String val = this.jedis.get(key);
            return new JSONObject(val);
        }

        @Override
        public boolean hasNext() {
            return this.keysIterator.hasNext();
        }
        
    }
}
