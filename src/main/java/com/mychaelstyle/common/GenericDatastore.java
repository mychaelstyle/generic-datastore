/**
 * 
 */
package com.mychaelstyle.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Generic Datastore
 * @author Masanori Nakashima
 */
public class GenericDatastore {

    /**
     * Constructor
     */
    public GenericDatastore() {
        super();
    }

    /** JSON parameter name : provider class name */
    public static final String PARAM_PROVIDER = "provider";

    /** Data store providers list */
    private List<Provider> providers = new ArrayList<Provider>();

    private String tableName;
    private String keyName;
    private String keyValue;
    private String subkeyName;
    private String subkeyValue;

    /**
     * 設定配列を指定してデータストアプロバイダを設定します.
     * 
     * 各要素JSONObjectのフォーマット<br>
     * <pre>
     * {
     *     "provider" : "プロバイダ実装クラス名",
     *     ...(プロバイダごとに固有の設定要素)...
     * }
     * </pre>
     * 
     * @param configs
     * @return
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws JSONException
     * @throws ConfigurationException
     */
    public GenericDatastore withProviders(final JSONArray configs)
            throws ClassNotFoundException, InstantiationException,
            IllegalAccessException, JSONException, ConfigurationException{
        for(int pos=0; pos<configs.length(); pos++){
            this.withProvider(configs.getJSONObject(pos));
        }
        return this;
    }

    /**
     * 設定を渡してデータストアプロバイダを追加.
     * 
     * JSONObjectのフォーマット<br>
     * <pre>
     * {
     *     "provider" : "プロバイダ実装クラス名",
     *     ...(プロバイダごとに固有の設定要素)...
     * }
     * </pre>
     * 
     * @param config
     * @return
     * @throws ConfigurationException
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public GenericDatastore withProvider(final JSONObject config)
            throws ConfigurationException, ClassNotFoundException,
            InstantiationException, IllegalAccessException {
        String cname = config.getString(PARAM_PROVIDER);
        Class<?> clazz = Class.forName(cname);
        Provider provider = (Provider) clazz.newInstance();
        provider.connect(config);
        this.providers.add(provider);
        return this;
    }

    private Provider selectProvider(){
        return this.providers.get(0);
    }

    /**
     * 問い合わせるデータベーステーブル名を設定します.
     * 
     * @param table
     * @return
     */
    public GenericDatastore withTable(final String table) {
        this.tableName = table;
        return this;
    }

    /**
     * 問い合わせる主キーを設定します.
     * 
     * @param key
     * @param value
     * @return
     */
    public GenericDatastore withKey(final String key, final String value) {
        this.keyName = key;
        this.keyValue = value;
        return this;
    }

    /**
     * 問い合わせる主キー名を設定します.
     * @param key
     * @return
     */
    public GenericDatastore withKeyName(final String key){
        this.keyName = key;
        return this;
    }

    /**
     * 問い合わせる副キーを設定します.
     * 
     * @param key
     * @param value
     * @return
     */
    public GenericDatastore withSubkey(final String key, final String value) {
        this.subkeyName = key;
        this.subkeyValue = value;
        return this;
    }

    /**
     * 問い合わせる副キー名を設定します.
     * 
     * @param key
     * @return
     */
    public GenericDatastore withSubkeyName(final String key){
        this.subkeyName = key;
        return this;
    }

    /**
     * 現在の問い合わせ設定をリセットします.
     */
    private void reset(){
        this.keyName = null;
        this.keyValue = null;
        this.subkeyName = null;
        this.subkeyValue = null;
    }

    /**
     * 実行時に設定されているテーブル名、主キー、副キーでレコードを取得.
     * @return JSONObject
     * @throws ConnectionException
     * @throws ConfigurationException
     * @throws OperationException
     */
    public JSONObject get() throws ConnectionException, ConfigurationException, OperationException {
        return this.get(true);
    }

    /**
     * 実行時に設定されているテーブル名、主キー、副キーでレコードを取得し、reset=trueであれば問い合わせ後に条件をリセット.
     * @return JSONObject
     * @param reset
     * @return JSONObject
     * @throws ConnectionException
     * @throws ConfigurationException
     * @throws OperationException
     */
    public JSONObject get(boolean reset) throws ConnectionException, ConfigurationException, OperationException {
        try {
            return this.selectProvider().withTable(this.tableName).withKey(this.keyName, this.keyValue)
                .withSubkey(this.subkeyName, this.subkeyValue).get();
        } finally {
            if(reset){
                this.reset();
            }
        }
    }

    private JSONObject getCurrentRecord(final JSONObject record)
            throws ConnectionException, ConfigurationException, OperationException {
        JSONObject cur = null;
        if(null!=this.keyName && record.has(this.keyName)) {
            String keyValue = record.getString(this.keyName);
            this.withKey(this.keyName, keyValue);
            if(null!=this.subkeyName && record.has(this.subkeyName)){
                String subkey = record.getString(this.subkeyName);
                this.withSubkey(this.subkeyName, subkey);
            }
            cur = this.get(false);
        }
        return cur;
    }

    /**
     * 実行時に設定されているテーブル名、主キー、副キーに対してレコードを登録.
     * 
     * @param record
     * @throws ConnectionException
     * @throws ConfigurationException
     * @throws OperationException
     */
    public void put(final JSONObject record)
            throws ConnectionException, ConfigurationException, OperationException {
        JSONObject cur = this.getCurrentRecord(record);
        int pos = 0;
        try {
            for(Provider provider : this.providers){
                provider.withTable(this.tableName).withKey(this.keyName, this.keyValue)
                .withSubkey(this.subkeyName, this.subkeyValue).put(record);
                pos++;
            }
        } catch(OperationException | ConnectionException | ConfigurationException e){
            e.printStackTrace();
            if(null!=cur){
                int c=0;
                for(Provider provider : this.providers){
                    if(pos==c){
                        break;
                    }
                    provider.put(cur);
                }
            }
            throw e;
        } finally {
            this.reset();
        }
    }

    /**
     * 実行時に設定されているテーブル名、主キー、副キーに対してレコードを更新.
     * 渡されたrecord JSONObjectに含まれていないキーに対しては更新も削除もおこないません。
     * @param record
     * @throws ConnectionException
     * @throws ConfigurationException
     * @throws OperationException
     */
    public void update(final JSONObject record)
            throws ConnectionException, ConfigurationException, OperationException {
        JSONObject cur = this.getCurrentRecord(record);
        int pos = 0;
        try {
            for(Provider provider : this.providers){
                provider.withTable(this.tableName).withKey(this.keyName, this.keyValue)
                .withSubkey(this.subkeyName, this.subkeyValue).update(record);
                pos++;
            }
        } catch(OperationException | ConnectionException | ConfigurationException e){
            e.printStackTrace();
            if(null!=cur){
                @SuppressWarnings("unchecked")
                Set<String> keys = record.keySet();
                JSONObject rollback = new JSONObject();
                for(String key:keys){
                    if(cur.has(key)){
                        rollback.put(key, cur.get(key));
                    }
                }
                int c=0;
                for(Provider provider : this.providers){
                    if(pos==c){
                        break;
                    }
                    provider.update(cur);
                }
            }
            throw e;
        } finally {
            this.reset();
        }
    }

    /**
     * 実行時に設定されているテーブル名、主キー、副キーで特定できるレコードを削除.
     * @throws ConnectionException
     * @throws ConfigurationException
     * @throws OperationException
     */
    public void delete() throws ConnectionException, ConfigurationException, OperationException {
        JSONObject cur = this.get(false);
        int pos = 0;
        try {
            for(Provider provider : this.providers){
                provider.withTable(this.tableName)
                    .withKey(this.keyName, this.keyValue)
                    .withSubkey(this.subkeyName, this.subkeyValue).delete();
                pos++;
            }
        } catch(OperationException | ConnectionException | ConfigurationException e){
            e.printStackTrace();
            if(null!=cur){
                int c=0;
                for(Provider provider : this.providers){
                    if(pos==c){
                        break;
                    }
                    provider.withTable(this.tableName).withKey(this.keyName, this.keyValue)
                        .withSubkey(this.subkeyName, this.subkeyValue).put(cur);
                }
            }
            throw e;
        } finally {
            this.reset();
        }
    }

    /**
     * 非同期でレコードを登録します.
     * 
     * @param tableName
     * @param record
     * @param keyName
     * @param subKeyName
     * @param callback
     * @throws ConnectionException
     * @throws ConfigurationException
     * @throws OperationException
     */
    public void asyncPut(final String tableName, final JSONObject record,
            final String keyName, final String subKeyName, final Callback callback)
            throws ConnectionException, ConfigurationException, OperationException {
        new Thread(new Runnable(){
            @Override
            public void run(){
                try {
                    withTable(tableName).withKey(keyName,keyValue).withSubkey(subkeyName, subkeyValue)
                    .put(record);
                } catch(Throwable e){
                    callback.handleException(e);
                }
            }
        }).start();
    }

    /**
     * 複数のキーを指定して一度に複数のレコードを取得.
     * 
     * JSONArrayの要素JSONObjectフォーマット<br>
     * <pre>
     * {
     *     "table" : "テーブル名",
     *     "key" : "主キーフィールド名",
     *     "subkey" : "副キーフィールド名",
     *     "data" : {
     *         "フィールド名" : "値",
     *         ...
     *     }
     * }
     * </pre>
     * 
     * @param conditions
     * @return
     * @throws ConnectionException
     * @throws ConfigurationException
     * @throws OperationException
     */
    public JSONObject batchGet(JSONArray conditions)
            throws ConnectionException, ConfigurationException, OperationException {
        try {
            JSONArray realConditions = new JSONArray();
            for(int num=0; num<conditions.length(); num++){
                JSONObject obj = conditions.getJSONObject(num);
                JSONObject cond = new JSONObject(obj.toString());
                cond.put("table", cond.getString("table"));
                realConditions.put(cond);
            }
            return this.selectProvider().batchGet(realConditions);
        } finally {
            this.reset();
        }
    }

    /**
     * 一括でデータの書き込みをおこないます.
     * このメソッドは複数プロバイダに対してデータの一貫性を保証しません。
     * 
     * JSONArrayの要素JSONObjectフォーマット<br>
     * <pre>
     * {
     *     "table" : "テーブル名",
     *     "key" : "主キーフィールド名",
     *     "subkey" : "副キーフィールド名",
     *     "action" : "put/delete"
     *     "data" : {
     *         "フィールド名" : "値",
     *         ...
     *     }
     * }
     * </pre>
     * 
     * @param jsonArray
     * @throws ConnectionException
     * @throws ConfigurationException
     * @throws OperationException
     */
    public void batchWrite(JSONArray jsonArray)
            throws ConnectionException, ConfigurationException, OperationException {
        try {
            JSONArray realConditions = new JSONArray();
            for(int num=0; num<jsonArray.length(); num++){
                JSONObject obj = jsonArray.getJSONObject(num);
                JSONObject cond = new JSONObject(obj.toString());
                cond.put("table", cond.getString("table"));
                realConditions.put(cond);
            }
            for(Provider provider : this.providers){
                provider.batchWrite(jsonArray);
            }
        } finally {
            this.reset();
        }
    }

    /**
     * データをフルスキャンして問い合わせ結果セットを取得
     * 
     * conditionsのフォーマット<br>
     * <pre>
     * {
     *     "フィールド名" : {
     *         "operator" : "</>/<=/>=/=",
     *         "value" : "値"
     *     },
     *     ...
     * }
     * </pre>
     * 
     * @param conditions
     * @param fields
     * @param limit
     * @return
     * @throws ConnectionException
     * @throws ConfigurationException
     * @throws OperationException
     */
    public ResultSet scan(JSONObject conditions,List<String> fields)
            throws ConnectionException, ConfigurationException, OperationException {
        try {
            return this.selectProvider().withTable(this.tableName).scan(conditions, fields);
        } finally {
            this.reset();
        }
    }

    /**
     * データを指定条件で問い合わせて結果セットを取得
     * 
     * conditionsのフォーマット<br>
     * <pre>
     * {
     *     "フィールド名" : {
     *         "operator" : "</>/<=/>=/=",
     *         "value" : "値"
     *     },
     *     ...
     * }
     * </pre>
     * 
     * @param conditions
     * @param fields
     * @param limit
     * @return
     * @throws ConnectionException
     * @throws ConfigurationException
     * @throws OperationException
     */
    public ResultSet query(JSONObject conditions,List<String> fields)
            throws ConnectionException, ConfigurationException, OperationException {
        try {
            return this.selectProvider().withTable(this.tableName)
                .query(conditions, fields);
        } finally {
            this.reset();
        }
    }

    /**
     * asyncPutで利用するコールバックインターフェース.
     * 
     * @author Masanori Nakashima
     */
    public interface Callback {
        /**
         * データ更新完了時に呼び出されるメソッド
         */
        public void callback();
        /**
         * 更新で発生した例外のハンドラ
         * @param e
         */
        public void handleException(Throwable e);
    }

    /**
     * 設定に関する例外
     * @author Masanori Nakashima
     *
     */
    public static class ConfigurationException extends Exception {
        private static final long serialVersionUID = 3251867995542305432L;
        public ConfigurationException(String message, Throwable cause,
                boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
        public ConfigurationException(String message, Throwable cause) {
            super(message, cause);
        }
        public ConfigurationException(String message) {
            super(message);
        }
        public ConfigurationException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * 接続に関する例外
     * @author Masanori Nakashima
     */
    public static class ConnectionException extends Exception {
        private static final long serialVersionUID = 3251867995542305432L;
        public ConnectionException(String message, Throwable cause,
                boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
        public ConnectionException(String message, Throwable cause) {
            super(message, cause);
        }
        public ConnectionException(String message) {
            super(message);
        }
        public ConnectionException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * 操作例外
     * @author Masanori Nakashima
     */
    public static class OperationException extends Exception {
        private static final long serialVersionUID = 3251867995542305432L;
        public OperationException(String message, Throwable cause,
                boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
        public OperationException(String message, Throwable cause) {
            super(message, cause);
        }
        public OperationException(String message) {
            super(message);
        }
        public OperationException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * DataStore プロバイダのインターフェース
     * @author Masanori Nakashima
     */
    public interface Provider {
        /** JSONフィールド名 : テーブル名 */
        public static final String FIELD_TABLE = "table";
        /** JSONフィールド名 : データ */
        public static final String FIELD_DATA = "data";
        /** JSONフィールド名 : アクション */
        public static final String FIELD_ACTION = "action";
        /** アクション : 削除 */
        public static final String ACTION_DELETE = "delete";
        /** アクション : 登録 */
        public static final String ACTION_PUT = "put";
        /** JSONフィールド名 : 主キー名 */
        public static final String FIELD_KEY = "key";
        /** JSONフィールド名 : 副キー名 */
        public static final String FIELD_SUBKEY = "subkey";
        /** JSONフィールド名 : 値 */
        public static final String FIELD_VALUE = "value";
        /** JSON value name */
        public static final String NAME_CONDITION_VAL = "value";
        /** JSON operator name */
        public static final String NAME_CONDITION_OPERATOR = "operator";

        /**
         * 接続設定をJSON形式で受け取ってデータストアへの接続処理をおこなう
         * @param config
         * @return
         * @throws ConfigurationException
         */
        public Provider connect(final JSONObject config) throws ConfigurationException;
        /**
         * 問い合わせ先のテーブル名を設定する
         * @param tableName
         * @return
         */
        public Provider withTable(final String tableName);
        /**
         * 問い合わせの主キーとその値を設定する
         * @param key
         * @param value
         * @return
         */
        public Provider withKey(final String key, final String value);
        /**
         * 問い合わせの副キーとその値を設定する.
         * @param key
         * @param value
         * @return
         */
        public Provider withSubkey(final String key, final String value);
        /**
         * 実行時に設定されているテーブル名、主キー、副キー情報からレコードを一意に取得する.
         * @return JSONObject
         * @throws ConnectionException
         * @throws ConfigurationException
         * @throws OperationException
         */
        public JSONObject get() throws ConnectionException, ConfigurationException, OperationException;
        /**
         * 実行時に設定されているテーブル名、主キー、副キー情報に基づいてレコードを一意に削除する.
         * @throws ConnectionException
         * @throws ConfigurationException
         * @throws OperationException
         */
        public void delete() throws ConnectionException, ConfigurationException, OperationException;
        /**
         * 実行時に指定されているテーブル、主キー名、副キー名に基づいてレコードを登録する.
         * @param record JSONObject
         * @throws ConnectionException
         * @throws ConfigurationException
         * @throws OperationException
         */
        public void put(final JSONObject record) throws ConnectionException, ConfigurationException, OperationException;
        /**
         * 実行時に指定されているテーブル名、主キー、副キーに基づいてレコードを更新する.
         * 引数オブジェクト存在しないフィールドは更新も削除もおこなわない.
         * @param record JSONObject
         * @throws ConnectionException
         * @throws ConfigurationException
         * @throws OperationException
         */
        public void update(final JSONObject record) throws ConnectionException, ConfigurationException, OperationException;
        /**
         * JSONArray形式で条件を指定して複数の条件に基づく複数のレコードを一度に取得する.
         * 
         * JSONArrayの要素JSONObjectフォーマット<br>
         * <pre>
         * {
         *     "table" : "テーブル名",
         *     "key" : "主キーフィールド名",
         *     "subkey" : "副キーフィールド名",
         *     "data" : {
         *         "フィールド名" : "値",
         *         ...
         *     }
         * }
         * </pre>
         * 
         * @param conditions
         * @return
         * @throws ConnectionException
         * @throws ConfigurationException
         * @throws OperationException
         */
        public JSONObject batchGet(JSONArray conditions) throws ConnectionException, ConfigurationException, OperationException;
        /**
         * JSONArray形式で条件や値やアクションを指定して一度の操作で書き込み・削除をおこなう.
         * アクションputが指定されていない場合、指定キーのレコードが存在しないなら新規作成、存在するなら上書き更新を実行する.
         * JSONArrayの要素JSONObjectフォーマット<br>
         * <pre>
         * {
         *     "table" : "テーブル名",
         *     "key" : "主キーフィールド名",
         *     "subkey" : "副キーフィールド名",
         *     "action" : "delete/put",
         *     "data" : {
         *         "フィールド名" : "値",
         *         ...
         *     }
         * }
         * </pre>
         * 
         * @param jsonArray
         * @throws ConnectionException
         * @throws ConfigurationException
         * @throws OperationException
         */
        public void batchWrite(JSONArray jsonArray) throws ConnectionException, ConfigurationException, OperationException;
        /**
         * 指定テーブルのスキャンを実行してResultSetの実装クラスインスタンスを取得.
         * 
         * conditionsのフォーマット<br>
         * <pre>
         * {
         *     "フィールド名" : {
         *         "operator" : "</>/<=/>=/=",
         *         "value" : "値"
         *     },
         *     ...
         * }
         * </pre>
         * 
         * @param conditions 取得する条件
         * @param fields 取得するフィールド名リスト
         * @param limit バッファ数
         * @return
         * @throws ConnectionException
         * @throws ConfigurationException
         * @throws OperationException
         */
        public ResultSet scan(JSONObject conditions,List<String> fields) throws ConnectionException, ConfigurationException, OperationException;
        /**
         * 指定テーブルのキー条件を指定してクエリーを実行しResultSetの実装クラスインスタンスを取得.
         * 
         * conditionsのフォーマット<br>
         * <pre>
         * {
         *     "フィールド名" : {
         *         "operator" : "</>/<=/>=/=",
         *         "value" : "値"
         *     },
         *     ...
         * }
         * </pre>
         * 
         * @param conditions
         * @param fields
         * @param limit
         * @return
         * @throws ConnectionException
         * @throws ConfigurationException
         * @throws OperationException
         */
        public ResultSet query(JSONObject conditions,List<String> fields) throws ConnectionException, ConfigurationException, OperationException;
    }

    /**
     * データストアプロバイダの基底抽象クラス
     * @author Masanori Nakashima
     */
    public static abstract class BaseProvider implements Provider {
        /** テーブル名 */
        protected String tableName = null;
        /** 主キーフィールド名 */
        protected String keyName = null;
        /** 主キー値 */
        protected String keyValue = null;
        /** 副キーフィールド名 */
        protected String subkeyName = null;
        /** 副キー値 */
        protected String subkeyValue = null;
        /* (non-Javadoc)
         * @see com.mychaelstyle.common.GenericDatastore.Provider#withTable(java.lang.String)
         */
        @Override
        public Provider withTable(String tableName) {
            this.tableName = tableName;
            return this;
        }
        /* (non-Javadoc)
         * @see com.mychaelstyle.common.GenericDatastore.Provider#withKey(java.lang.String, java.lang.String)
         */
        @Override
        public Provider withKey(String key, String value) {
            this.keyName = key;
            this.keyValue = value;
            return this;
        }
        /* (non-Javadoc)
         * @see com.mychaelstyle.common.GenericDatastore.Provider#withSubkey(java.lang.String, java.lang.String)
         */
        @Override
        public Provider withSubkey(String key, String value) {
            this.subkeyName = key;
            this.subkeyValue = value;
            return this;
        }

    }

    /**
     * KVS スキャンとクエリーの結果セットインターフェース
     * @author Masanori Nakashima
     */
    public interface ResultSet {
        /**
         * 次の結果レコードをJSONObjectで取得
         * @return
         */
        public JSONObject next() throws ConfigurationException, ConnectionException, OperationException;
        /**
         * 次の結果レコードがあるか確認
         * @return
         */
        public boolean hasNext() throws ConfigurationException, ConnectionException, OperationException;
    }


}
