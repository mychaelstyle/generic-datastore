package com.mychaelstyle.common.datastore.aws;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.json.JSONObject;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.mychaelstyle.common.GenericDatastore.ConfigurationException;
import com.mychaelstyle.common.GenericDatastore.ConnectionException;
import com.mychaelstyle.common.GenericDatastore.OperationException;
import com.mychaelstyle.common.datastore.AwsDynamoDB;

public class QResultSet implements com.mychaelstyle.common.GenericDatastore.ResultSet {

    /**
     * configuration to connect
     */
    protected JSONObject config = null;
    /**
     * last key of scan
     */
    protected Map<String,AttributeValue> lastKey = null;
    /**
     * QueryRequest
     */
    protected QueryRequest request;
    /**
     * result queue
     */
    Queue<JSONObject> resultQueue = null;

    /**
     * constructor
     * @param request
     * @throws ConfigurationException 
     */
    public QResultSet(JSONObject config, QueryRequest request) throws ConfigurationException{
        this.request = request;
        this.config = config;
        this.load();
    }

    /**
     * get next JSONObject
     * @return
     * @throws ConfigurationException 
     * @throws ConnectionException 
     * @throws OperationException 
     */
    public JSONObject next() throws ConfigurationException,ConnectionException,OperationException{
        if(this.hasNext()) {
            return this.resultQueue.poll();
        }
        return null;
    }

    /**
     * has next
     * @return
     * @throws ConfigurationException 
     * @throws ConnectionException 
     * @throws OperationException 
     */
    public boolean hasNext() throws ConfigurationException,ConnectionException,OperationException {
        if(this.resultQueue == null || this.resultQueue.size()==0) {
            if(null == this.lastKey){
                return false;
            }
            this.load();
            if(null == this.resultQueue || this.resultQueue.size()==0){
                return false;
            } else {
                return true;
            }
        }
        return true;
    }

    /**
     * get items
     * @return
     * @throws ConfigurationException 
     */
    private void load() throws ConfigurationException{
       AmazonDynamoDBClient c = AwsDynamoDB.getClient(this.config);
            this.request.setExclusiveStartKey(this.lastKey);
            QueryResult result = null;
            int counter=0;
            while(true){
                try {
                    result = c.query(this.request);
                    break;
                } catch(Throwable e){
                    if(counter>10){ throw e; }
                    try {
                        Thread.sleep(10000);
                    } catch(Throwable ex){
                        // through
                    }
                }
                counter++;
            }
            List<Map<String,AttributeValue>> resList = result.getItems();
            if(resList.size()==0){
                this.resultQueue = null;
                this.lastKey = null;
                return;
            }
            this.resultQueue = new LinkedList<JSONObject>();
            for(Map<String,AttributeValue> item : resList){
                JSONObject json = new JSONObject();
                for(Map.Entry<String, AttributeValue> field : item.entrySet()){
                    String key = field.getKey();
                    AttributeValue atv = field.getValue();
                    String value = atv.getS();
                    if(null!=value){
                        json.put(key, value);
                    } else {
                        value = atv.getN();
                        if(null!=value){
                            json.put(key, value);
                        }
                    }
                }
                this.resultQueue.add(json);
            }
            this.lastKey = result.getLastEvaluatedKey();
        }


}
