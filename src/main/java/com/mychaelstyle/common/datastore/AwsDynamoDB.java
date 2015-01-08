package com.mychaelstyle.common.datastore;

import com.mychaelstyle.common.GenericDatastore;

import java.util.*;

import org.json.JSONArray;
import org.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.mychaelstyle.common.GenericDatastore.ConfigurationException;
import com.mychaelstyle.common.GenericDatastore.ConnectionException;
import com.mychaelstyle.common.GenericDatastore.OperationException;
import com.mychaelstyle.common.GenericDatastore.Provider;
import com.mychaelstyle.common.GenericDatastore.ResultSet;
import com.mychaelstyle.common.datastore.aws.QResultSet;
import com.mychaelstyle.common.datastore.aws.SResultSet;

/**
 * Amazon DynamoDB Wrapper 
 * 
 * 以下の環境変数の設定が必要です。<br>
 * <br>
 * METALICS_MODE ... 動作モード（development, testing, staging, production)<br>
 * METALICS_COUNTRY ... 動作させている国を指定。jp, us, ...etc. デフォルトjp<br>
 * AWS_ACCESS_KEY ... AWS APIへのアクセスキー<br>
 * AWS_SECRET_KEY ... AWS APIへのシークレットキー<br>
 * AWS_ENDPOINT_DYNAMODB ... DynamoDBエンドポイント e.g. dynamodb.ap-northeast-1.amazonaws.com<br>
 * <br>
 * 
 * @author Masanori Nakashima
 */
public class AwsDynamoDB extends GenericDatastore.BaseProvider implements GenericDatastore.Provider {
    /** データタイプ：文字列 */
    public static final Integer TYPE_STRING = 0;
    /** データタイプ：数字 */
    public static final Integer TYPE_NUMBER = 1;
    /** データタイプ バイナリ */
    public static final Integer TYPE_BINARY = 2;
    /** retry max count */
    public static final int RETRY_MAX = 10;
    /** scan limit default : 20 */
    public static final int SCAN_LIMIT_DEFAULT = 25;

    /** JSON設定項目 エンドポイント */
    public static final String CONFIG_ENDPOINT = "endpoint";
    /** JSON設定項目 アクセスキー */
    public static final String CONFIG_ACCESS_KEY = "access_key";
    /** JSON設定項目 シークレットキー */
    public static final String CONFIG_SECRET_KEY = "secret_key";

    /** Logger by logback */
    private static Logger logger = LoggerFactory.getLogger("com.mychaelstyle.common.datastore");

    /**
     * 設定JSON
     */
    private JSONObject config = null;
    //
    // member fields
    //
    /**
     * dynamodb client v2
     */
    private static AmazonDynamoDBClient client;

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#connect(org.json.JSONObject)
     */
    @Override
    public Provider connect(JSONObject config) throws ConfigurationException {
        this.config = config;
        AwsDynamoDB.getClient(config);
        return this;
    }

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#get()
     */
    @Override
    public JSONObject get() throws ConnectionException, ConfigurationException,
            OperationException {
        return AwsDynamoDB.get(this.keyName, this.keyValue, this.subkeyName,
                this.subkeyValue, this.tableName);
    }

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#delete()
     */
    @Override
    public void delete() throws ConnectionException, ConfigurationException,
            OperationException {
        AwsDynamoDB.delete(this.keyName, this.keyValue, this.subkeyName, this.subkeyValue, this.tableName);
    }

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#put(org.json.JSONObject)
     */
    @Override
    public void put(JSONObject record) throws ConnectionException,
            ConfigurationException, OperationException {
        AwsDynamoDB.put(record, this.tableName);
    }

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#update(org.json.JSONObject)
     */
    @Override
    public void update(JSONObject record) throws ConnectionException,
            ConfigurationException, OperationException {
        record.put(this.keyName, this.keyValue);
        if(null!=this.subkeyName && this.subkeyName.length()>0){
            record.put(this.subkeyName, this.subkeyValue);
        }
        AwsDynamoDB.update(record, this.tableName, this.keyName, this.subkeyName);
    }

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#batchGet(org.json.JSONArray)
     */
    @Override
    public JSONObject batchGet(JSONArray conditions)
            throws ConnectionException, ConfigurationException,
            OperationException {
        try {
            return AwsDynamoDB.batchGetItems(conditions);
        } catch(Exception e){
            throw new OperationException(e);
        }
    }

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#batchWrite(org.json.JSONArray)
     */
    @Override
    public void batchWrite(JSONArray jsonArray) throws ConnectionException,
            ConfigurationException, OperationException {
        try {
            AwsDynamoDB.batchWriteItems(jsonArray);
        } catch (Exception e) {
            throw new OperationException(e);
        }
    }

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#scan(org.json.JSONObject, java.util.List, int)
     */
    @Override
    public ResultSet scan(JSONObject conditions, List<String> fields)
            throws ConnectionException, ConfigurationException,
            OperationException {
        return AwsDynamoDB.scan(this.config, this.tableName, conditions, fields);
    }

    /* (non-Javadoc)
     * @see com.mychaelstyle.common.GenericDatastore.Provider#query(org.json.JSONObject, java.util.List, int)
     */
    @Override
    public ResultSet query(JSONObject conditions, List<String> fields)
            throws ConnectionException, ConfigurationException,
            OperationException {
        return AwsDynamoDB.query(this.config, this.tableName, conditions, fields);
    }

    /**
     * get DynamoDB client
     * @return AmazonDynamoDBClient
     */
    private static AmazonDynamoDBClient getClient() throws ConfigurationException {
        if(null==client){
            throw new ConfigurationException("No dynamo db client created.");
        }
        return client;
    }

    /**
     * get dynamo db client
     * @param config
     * @return
     * @throws ConfigurationException
     */
    public static AmazonDynamoDBClient getClient(JSONObject config) throws ConfigurationException {
        if(null == client){
            AWSCredentials credentials = getCredentials(config);
            client = new AmazonDynamoDBClient(credentials);
            String endpoint = config.getString(CONFIG_ENDPOINT);
            if(null==endpoint) throw new ConfigurationException(CONFIG_ENDPOINT);
            client.setEndpoint(endpoint);
        }
        return client;
    }

    /**
     * create aws credentials
     * TODO : 後でまとめる
     * @param config
     * @return
     * @throws ConfigurationException 
     */
    private static AWSCredentials getCredentials(JSONObject config) throws ConfigurationException{
        String accessKey = config.getString(CONFIG_ACCESS_KEY);
        String secretKey = config.getString(CONFIG_SECRET_KEY);
        if(accessKey==null) throw new ConfigurationException(CONFIG_ACCESS_KEY);
        if(secretKey==null) throw new ConfigurationException(CONFIG_SECRET_KEY);
        return new BasicAWSCredentials(accessKey,secretKey);
    }

    /**
     * create WriteRequest
     * @param json
     * @return
     */
    private static WriteRequest createWriteRequest(JSONObject json){
        String action = json.getString(FIELD_ACTION);
        JSONObject data   = json.getJSONObject(FIELD_DATA);
        if(ACTION_DELETE.equalsIgnoreCase(action)){
            DeleteRequest delReq = new DeleteRequest();
            for(Object ko : data.keySet()){
                String k = (String) ko;
                Object v = data.get(k);
                if(v instanceof Integer){
                    delReq.addKeyEntry(k, new AttributeValue().withN(((Integer)v).toString()));
                } else if(v instanceof String){
                    delReq.addKeyEntry(k, new AttributeValue().withS((String)v));
                } else {
                    String str = JSONObject.valueToString(v);
                    delReq.addKeyEntry(k, new AttributeValue().withS(str));
                }
            }
            return new WriteRequest(delReq);
        } else {
            PutRequest putReq = new PutRequest();
            for(Object ko : data.keySet()){
                String k = (String) ko;
                Object v = data.get(k);
                if(v instanceof Integer){
                    putReq.addItemEntry(k, new AttributeValue().withN(((Integer)v).toString()));
                } else if(v instanceof String){
                    putReq.addItemEntry(k, new AttributeValue().withS((String)v));
                } else {
                    String str = JSONObject.valueToString(v);
                    putReq.addItemEntry(k, new AttributeValue().withS(str));
                }
            }
            return new WriteRequest(putReq);
        }
    }

    /**
     * batch write
     * @param jsonArray
     * @throws Exception 
     */
    public static void batchWriteItems(JSONArray jsonArray) throws Exception{
        Map<String,List<WriteRequest>> itemsMap = new HashMap<String,List<WriteRequest>>();
        for(int num=0; num<jsonArray.length(); num++){
            JSONObject json = jsonArray.getJSONObject(num);
            String table  = json.getString(FIELD_TABLE);
            WriteRequest item = AwsDynamoDB.createWriteRequest(json);
            List<WriteRequest> trRequests = new ArrayList<WriteRequest>();
            if(itemsMap.containsKey(table)){
                trRequests = itemsMap.get(table);
            }
            trRequests.add(item);
            itemsMap.put(table, trRequests);
        }
        BatchWriteItemRequest request = new BatchWriteItemRequest();
        request.setRequestItems(itemsMap);
        int counter = 1;
        while(true){
            try {
                getClient().batchWriteItem(request);
                break;
            }catch(Throwable e){
                if(counter>RETRY_MAX){
                    StringBuffer buf = new StringBuffer();
                    buf.append("Fail to batch write : ").append(e.getMessage()).append("\n");
                    buf.append(jsonArray.toString()).append("\n");
                    buf.append(e.getStackTrace().toString());
                    logger.error(buf.toString());
                    throw new Exception(e);
                }
                sleepRandom(counter);
            }
            counter++;
        }
    }

    /**
     * update item
     * @param json
     * @param tableName
     * @param keyName
     * @throws ConfigurationException
     */
    public static void update(JSONObject json, String tableName, String keyName) throws ConfigurationException {
        AwsDynamoDB.update(json, tableName, keyName, null);
    }

    /**
     * update item
     * @param json
     * @param tableName
     * @param keyName
     * @param rangeName
     * @throws ConfigurationException
     */
    public static void update(JSONObject json, String tableName,
            String keyName, String rangeName) throws ConfigurationException {
        Map<String, AttributeValue> itemKeyMap = new HashMap<String, AttributeValue>();
        Map<String, AttributeValueUpdate> itemMap = new HashMap<String, AttributeValueUpdate>();
        @SuppressWarnings("unchecked")
        Iterator<String> iterator = json.keys();
        while(iterator.hasNext()){
            String key = iterator.next();
            Object val = json.get(key);
            if((null!=keyName && keyName.equalsIgnoreCase(key))
             || (null!=rangeName && rangeName.equalsIgnoreCase(key))){
                if(val instanceof Integer){
                    itemKeyMap.put(key, new AttributeValue().withN(String.valueOf(val)));
                } else if(val instanceof String){
                    itemKeyMap.put(key, new AttributeValue().withS((String)val));
                } else {
                    String v = val.toString();
                    if(null!=v && v.length()>0) {
                        itemKeyMap.put(key, new AttributeValue().withS(v));
                    }
                }
            } else {
                if(val instanceof Integer){
                    itemMap.put(key, new AttributeValueUpdate().withValue(new AttributeValue().withN(String.valueOf(val))));
                } else if(val instanceof String){
                    itemMap.put(key, new AttributeValueUpdate().withValue(new AttributeValue().withS((String)val)));
                } else {
                    String v = val.toString();
                    if(null!=v && v.length()>0) {
                        itemMap.put(key, new AttributeValueUpdate().withValue(new AttributeValue().withS(v)));
                    }
                }
            }
        }
        UpdateItemRequest itemRequest = new UpdateItemRequest().withTableName(tableName)
                .withKey(itemKeyMap)
                .withAttributeUpdates(itemMap);
        int counter = 1;
        while(true){
            try {
                getClient().updateItem(itemRequest);
                break;
            }catch(Throwable e){
                if(counter>RETRY_MAX){
                    StringBuffer buf = new StringBuffer();
                    buf.append("Fail to update :").append(e.getMessage()).append("\n");
                    buf.append("Table name=").append(tableName).append(" keyName=").append(keyName)
                    .append((null!=rangeName) ? " rangeName="+rangeName : "");
                    buf.append("\n");
                    buf.append(json.toString()).append("\n");
                    buf.append(e.getStackTrace().toString());
                    logger.error(buf.toString());
                    throw e;
                }
                sleepRandom(counter);
            }
            counter++;
        }
    }

    /**
     * put an item
     * @param json org.json.JSONObject
     * @param tableName String DynamoDB Table Name
     */
    public static void put(JSONObject json, String tableName) throws ConfigurationException {
        Map<String, AttributeValue> itemMap = new HashMap<String, AttributeValue>();
        @SuppressWarnings("unchecked")
        Iterator<String> iterator = json.keys();
        while(iterator.hasNext()){
            String key = iterator.next();
            Object val = json.get(key);
            if(val instanceof Integer){
                itemMap.put(key, new AttributeValue().withN(String.valueOf(val)));    
            } else if(val instanceof String){
                itemMap.put(key, new AttributeValue().withS((String)val));    
            } else {
                String v = val.toString();
                if(null!=v && v.length()>0) {
                    itemMap.put(key, new AttributeValue().withS(v));    
                }
            }
        }
        PutItemRequest itemRequest = new PutItemRequest().withTableName(tableName).withItem(itemMap);
        int counter = 1;
        while(true){
            try {
                getClient().putItem(itemRequest);
                break;
            }catch(Throwable e){
                if(counter>RETRY_MAX){
                    StringBuffer buf = new StringBuffer();
                    buf.append("Fail to put request : ").append(e.getMessage()).append("\n");
                    buf.append("Table name=").append(tableName).append("\n");
                    buf.append(json.toString()).append("\n");
                    buf.append(e.getStackTrace().toString());
                    logger.error(buf.toString());
                    throw e;
                }
                sleepRandom(counter);
            }
            counter++;
        }
    }

    /**
     * batch get items
     * @param conditions
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public static JSONObject batchGetItems(JSONArray conditions) throws Exception {
        BatchGetItemRequest request = new BatchGetItemRequest();
        Map<String,KeysAndAttributes> keysMap = new HashMap<String,KeysAndAttributes>();
        for(int num=0; num<conditions.length(); num++){
            JSONObject condition = conditions.getJSONObject(num);
            Map<String,AttributeValue> map = new HashMap<String,AttributeValue>();
            String table = condition.getString(FIELD_TABLE);
            JSONObject cond = condition.getJSONObject(FIELD_DATA);
            Set<String> fields = cond.keySet();
            for(Object field : fields){
                String fname = (String) field;
                Object value = cond.get(fname);
                if(value instanceof Integer){
                    map.put(fname, new AttributeValue().withN(value.toString()));
                } else {
                    map.put(fname, new AttributeValue().withS(value.toString()));
                }
            }
            if(keysMap.containsKey(table)){
                keysMap.get(table).withKeys(map);
            } else {
                KeysAndAttributes keys = new KeysAndAttributes().withKeys(map);
                keysMap.put(table, keys);
            }
        }
        for(String table : keysMap.keySet()){
            request.addRequestItemsEntry(table, keysMap.get(table));
        }
        int counter = 0;
        while(true){
            try {
                BatchGetItemResult result = getClient().batchGetItem(request);
                Map<String,List<Map<String,AttributeValue>>> resMap = result.getResponses();
                JSONObject retJson = new JSONObject();
                for(String table : resMap.keySet()){
                    List<Map<String,AttributeValue>> rows = resMap.get(table);
                    JSONArray jsonRows = new JSONArray();
                    for(Map<String,AttributeValue> row : rows){
                        JSONObject obj = new JSONObject();
                        for(String field : row.keySet()){
                            obj.put(field, row.get(field).getS());
                        }
                        jsonRows.put(obj);
                    }
                    retJson.put(table, jsonRows);
                }
                return retJson;
            } catch(Throwable e){
                if(counter>RETRY_MAX){
                    StringBuffer buf = new StringBuffer();
                    buf.append("Fail to batchGet ").append(e.getMessage()).append("\n");
                    buf.append(conditions.toString()).append("\n");
                    buf.append(e.getStackTrace().toString());
                    logger.error(buf.toString());
                }
                sleepRandom(counter);
            }
            counter++;
        }
    }

    /**
     * キーを指定してレコードを取得. テーブル名を変換しません.
     * 
     * @param keyName キーフィールド名
     * @param keyValue キー値
     * @param rangeName レンジキーフィールド名
     * @param rangeValue レンジキー値
     * @param tableName 対象テーブル名
     * @return
     * @throws ConfigurationException
     */
    public static JSONObject get(String keyName, Object keyValue,
            String rangeName, Object rangeValue, String tableName)
    throws ConfigurationException {
        String tTable = tableName;
        Map<String,AttributeValue> keyMap = new HashMap<String,AttributeValue>();
        AttributeValue keyValueAttr = new AttributeValue();
        if(keyValue instanceof Integer || keyValue instanceof Double){
            keyValueAttr.withN(keyValue.toString());
        } else {
            keyValueAttr.withS(keyValue.toString());
        }
        keyMap.put(keyName,keyValueAttr);
        if(null != rangeName && null != rangeValue){
            AttributeValue rangeValueAttr = new AttributeValue();
            if(rangeValue instanceof Integer || rangeValue instanceof Double){
                rangeValueAttr.withN(rangeValue.toString());
            } else {
                rangeValueAttr.withS(rangeValue.toString());
            }
            keyMap.put(rangeName,rangeValueAttr);
        }
        // request
        GetItemRequest getItemRequest = new GetItemRequest().withTableName(tTable).withKey(keyMap);
        GetItemResult result = null;
        int counter = 1;
        while(true){
            try {
                result = getClient().getItem(getItemRequest);
                break;
            }catch(Throwable e){
                if(counter>RETRY_MAX){
                    StringBuffer buf = new StringBuffer();
                    buf.append("Fail to get : ").append(e.getMessage()).append("\n");
                    buf.append("Table name=").append(tableName);
                    buf.append(" KeyName=").append(keyName)
                    .append(" Value=").append(keyValue.toString());
                    if(null!=rangeName){
                        buf.append("RangeName=").append(rangeName)
                        .append(" Value=").append((null!=rangeValue? rangeValue.toString() : ""));
                    }
                    buf.append(e.getStackTrace().toString());
                    logger.error(buf.toString());
                    throw e;
                }
                sleepRandom(counter);
            }
            counter++;
        }

        // create json
        JSONObject jsonObject = new JSONObject();
        Map<String,AttributeValue> resultMap = result.getItem();
        if(null==resultMap || resultMap.isEmpty()){
            return null;
        } else {
            for (Map.Entry<String, AttributeValue> item : resultMap.entrySet()) {
                String key = item.getKey();
                AttributeValue val = item.getValue();
                String value = val.getS();
                if(null!=value){
                    jsonObject.put(key, value);
                } else {
                    value = val.getN();
                    if(null!=value){
                        jsonObject.put(key, value);
                    }
                }
            }
            return jsonObject;
        }
    }

    /**
     * get an item
     * @param keyName
     * @param keyValue
     * @param tableName
     * @return
     * @throws ConfigurationException
     */
    public static JSONObject get(String keyName, Object keyValue, String tableName)
    throws ConfigurationException {
        return get(keyName,keyValue,null,null,tableName);
    }

    /**
     * キーを指定してレコードを削除. 環境ごとのテーブル名変換を自動でおこないません。
     * @param keyName キーフィールド名
     * @param keyValue キー値
     * @param rangeName レンジキーフィールド名
     * @param rangeValue レンジキー値
     * @param tableName 対象テーブル名
     * @throws ConfigurationException
     */
    public static void delete(String keyName, Object keyValue,
            String rangeName, Object rangeValue, String tableName)
    throws ConfigurationException {
        Map<String,AttributeValue> cond = new HashMap<String,AttributeValue>();
        AttributeValue keyAttr = new AttributeValue();
        if(keyValue instanceof Integer || keyValue instanceof Double){
            keyAttr.withN(keyValue.toString());
        } else {
            keyAttr.withS(keyValue.toString());
        }
        cond.put(keyName, keyAttr);
        if(null!=rangeName){
            AttributeValue rangeAttr = new AttributeValue();
            if(rangeValue instanceof Integer || rangeValue instanceof Double){
                rangeAttr.withN(rangeValue.toString());
            } else {
                rangeAttr.withS(rangeValue.toString());
            }
            cond.put(rangeName, rangeAttr);
        }
        DeleteItemRequest deleteItemRequest = new DeleteItemRequest(tableName,cond);
        getClient().deleteItem(deleteItemRequest);
        int counter = 1;
        while(true){
            try {
                getClient().deleteItem(deleteItemRequest);
                break;
            }catch(Throwable e){
                if(counter>RETRY_MAX){
                    StringBuffer buf = new StringBuffer();
                    buf.append("Fail to delete : ").append(e.getMessage()).append("\n");
                    buf.append("Table name=").append(tableName);
                    buf.append(" KeyName=").append(keyName).append(" Value=").append(keyValue.toString());
                    if(null!=rangeName){
                        buf.append("RangeName=").append(rangeName)
                        .append(" Value=").append(null!=rangeValue? rangeValue.toString() : "");
                    }
                    buf.append(e.getStackTrace().toString());
                    logger.error(buf.toString());
                    throw e;
                }
                sleepRandom(counter);
            }
            counter++;
        }
    }

    /**
     * delete item
     * @param keyName
     * @param keyValue
     * @param tableName
     * @throws ConfigurationException
     */
    public static void delete(String keyName, String keyValue, String tableName)
    throws ConfigurationException {
        delete(keyName,keyValue,null,null,tableName);
    }

    /**
     * 指定のテーブルをscan. 環境ごとのテーブル名自動変換を自動でおこなわない.
     * @param table 対象テーブル
     * @param conditions 条件JSON
     * @param fields 取得フィールドリスト
     * @param limit 一度の問い合わせで取得する上限レコード数
     * @return
     * @throws ConfigurationException 
     */
    public static ResultSet scan(JSONObject config, String table,
            JSONObject conditions,List<String> fields, int limit) throws ConfigurationException{
        Map<String,Condition> awsConditions = new HashMap<String,Condition>();
        if(null!=conditions){
            @SuppressWarnings("unchecked")
            Iterator<String> kIterator = (Iterator<String>) conditions.keys();
            while(kIterator.hasNext()) {
                String key = (String) kIterator.next();
                JSONObject json = conditions.getJSONObject(key);
                Condition cond = AwsDynamoDB.createCondition(json);
                awsConditions.put(key, cond);
            }
        }
        ScanRequest scanRequest = new ScanRequest()
           .withTableName(table)
           .withScanFilter(awsConditions)
           .withAttributesToGet(fields)
           .withLimit(limit);
        return new SResultSet(config, scanRequest);
    }

    /**
     * create Condition instance
     * @param json
     * @return
     */
    protected static Condition createCondition(JSONObject json) {
        String op = json.getString(NAME_CONDITION_OPERATOR);
        Condition cond = new Condition();
        ComparisonOperator eOp = ComparisonOperator.EQ;
        if(">".equalsIgnoreCase(op)){
            eOp = ComparisonOperator.GT;
        } else if(">=".equalsIgnoreCase(op)){
            eOp = ComparisonOperator.GE;
        } else if("<".equalsIgnoreCase(op)){
            eOp = ComparisonOperator.LT;
        } else if("<=".equalsIgnoreCase(op)){
            eOp = ComparisonOperator.LE;
        } else if("beginWith".equalsIgnoreCase(op)){
            eOp = ComparisonOperator.BEGINS_WITH;
        } else {
            eOp = ComparisonOperator.EQ;
        }
        cond.withComparisonOperator(eOp);

        Object value = json.get(NAME_CONDITION_VAL);
        if( value instanceof String){
            cond.withAttributeValueList(new AttributeValue().withS((String) value));
        } else {
            List<AttributeValue> attributes = new ArrayList<AttributeValue>();
            JSONObject vals = (JSONObject) value;
            @SuppressWarnings("unchecked")
            Iterator<String> ks = (Iterator<String>) vals.keys();
            while(ks.hasNext()){
                String k = (String) ks.next();
                String v = vals.getString(k);
                AttributeValue atr = new AttributeValue();
                atr.setS(v);
                attributes.add(atr);
            }
            cond.withAttributeValueList(attributes);
        }
        return cond;
    }

    /**
     * scan with limit 20
     * @param table
     * @param conditions
     * @param fields
     * @return
     * @throws ConfigurationException 
     */
    public static ResultSet scan(JSONObject config, String table, JSONObject conditions,List<String> fields) throws ConfigurationException{
        return AwsDynamoDB.scan(config, table,conditions,fields,AwsDynamoDB.SCAN_LIMIT_DEFAULT);
    }

    /**
     * 指定のテーブルの登録数を確認
     * @param tableName レコードをカウントする対象のテーブル名
     */
    public static void count(JSONObject config, String tableName){
        try {
            ResultSet result = AwsDynamoDB.scan(config, tableName,
                    new JSONObject(),
                    Arrays.asList("app_id","app_name"), 100);
            long count = 0;
            while(result.hasNext()){
                result.next();
                count++;
                if(count%100==0){
                    System.out.print(".");
                }
                if(count%5000==0){
                    System.out.print("  "+count);
                    System.out.println();
                }
            }
            System.out.println();
            System.out.print("Total app count = ");
            System.out.println(count);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * DynamoDBに指定の名前のテーブルを作成します。
     * @param name テーブル名
     * @param keyName ハッシュキー名
     * @param keyType ハッシュキーデータタイプ
     * @param rangeName レンジキー名 作成しない場合はnull
     * @param rangeType レンジキーデータタイプ 作成しない場合はnull
     */
    public static void createTable(String name, String keyName, Integer keyType,
            String rangeName, Integer rangeType) {
        try {
            String tableName = name;
            List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
            // primary key
            keySchema.add(new KeySchemaElement().withAttributeName(keyName)
                    .withKeyType(KeyType.HASH));
            ScalarAttributeType ht = (TYPE_BINARY==keyType)?ScalarAttributeType.B :
                (TYPE_NUMBER==keyType)?ScalarAttributeType.N : ScalarAttributeType.S;
            AttributeDefinition attrDef = new AttributeDefinition().withAttributeName(keyName)
                    .withAttributeType(ht);
            ProvisionedThroughput pt = new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(5L);
            // range key
            AttributeDefinition rangeDef = null;
            if(null != rangeName && rangeName.length()>0) {
                keySchema.add(new KeySchemaElement().withAttributeName(rangeName)
                    .withKeyType(KeyType.RANGE));
                ScalarAttributeType rt = (TYPE_BINARY==rangeType)?ScalarAttributeType.B :
                    (TYPE_NUMBER==rangeType)?ScalarAttributeType.N : ScalarAttributeType.S;
                rangeDef = new AttributeDefinition().withAttributeName(rangeName)
                        .withAttributeType(rt);
            }
            CreateTableRequest request = new CreateTableRequest(tableName, keySchema)
                .withAttributeDefinitions(attrDef).withProvisionedThroughput(pt);
            if(null!=rangeDef){
                request.withAttributeDefinitions(rangeDef);
            }
            AwsDynamoDB.getClient().createTable(request);
        } catch(RuntimeException e) {
        } catch(Exception e) {
        }
    }

    /**
     * DynamoDBから指定の名前のテーブルを削除します。
     * @param tableName 削除するテーブル名
     */
    public static void deleteTable(String tableName){
        try {
            String table = tableName;

            DeleteTableRequest request = new DeleteTableRequest(table);
            AwsDynamoDB.getClient().deleteTable(request);
        } catch(Throwable e){
            // through
        }
    }

    /**
     * query
     * @param table
     * @param conditions
     * @param fields
     * @param limit
     * @return
     * @throws ConfigurationException 
     */
    public static ResultSet query(JSONObject config, String table,
            JSONObject conditions,List<String> fields,int limit) throws ConfigurationException{
        Map<String,Condition> awsConditions = new HashMap<String,Condition>();
        if(null!=conditions){
            @SuppressWarnings("unchecked")
            Iterator<String> kIterator = (Iterator<String>) conditions.keys();
            while(kIterator.hasNext()) {
                String key = (String) kIterator.next();
                JSONObject json = conditions.getJSONObject(key);
                Condition cond = AwsDynamoDB.createCondition(json);
                awsConditions.put(key, cond);
            }
        }
        QueryRequest queryRequest = new QueryRequest()
           .withTableName(table)
           .withKeyConditions(awsConditions)
           .withAttributesToGet(fields)
           .withLimit(limit);
        return new QResultSet(config, queryRequest);
    }

    /**
     * query with limit 20
     * @param table
     * @param conditions
     * @param fields
     * @return
     * @throws ConfigurationException 
     */
    public static ResultSet query(JSONObject config, String table, JSONObject conditions,List<String> fields) throws ConfigurationException{
        return AwsDynamoDB.query(config, table,conditions,fields,AwsDynamoDB.SCAN_LIMIT_DEFAULT);
    }

    /**
     * sleep random mili seconds, 500 to 1500.
     * @param number
     */
    public static void sleepRandom(long number){
        long sleepmillis = (new Random().nextLong() % 1000)+500;
        try {
            Thread.sleep(number*sleepmillis);
        } catch (InterruptedException e) {
            logger.warn(e.getMessage());
        }
    }
}
