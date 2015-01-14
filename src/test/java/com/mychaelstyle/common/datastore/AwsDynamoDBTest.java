package com.mychaelstyle.common.datastore;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mychaelstyle.common.GenericDatastore.ResultSet;

public class AwsDynamoDBTest {

    /** テスト用テーブル名 */
    private static String TEST_TABLE = "test_lib_awsdynamodb";
    private AwsDynamoDB dynamodb = null;

    @Before
    public void setUp() throws Exception {
        try {
            AwsDynamoDB.createTable(TEST_TABLE, "key", 
                    AwsDynamoDB.TYPE_STRING,"subkey",
                    AwsDynamoDB.TYPE_STRING);
        } catch(Throwable e){
            fail(e.getMessage());
        }
        JSONObject config = new JSONObject();
        config.put("provider", "com.mychaelstyle.common.datastore.AwsDynamoDB");
        config.put(AwsDynamoDB.CONFIG_ACCESS_KEY,System.getenv("AWS_ACCESS_KEY"));
        config.put(AwsDynamoDB.CONFIG_SECRET_KEY,System.getenv("AWS_SECRET_KEY"));
        config.put(AwsDynamoDB.CONFIG_ENDPOINT, "dynamodb.ap-northeast-1.amazonaws.com");
        this.dynamodb = new AwsDynamoDB();
        this.dynamodb.connect(config);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void test() {
        JSONObject record = new JSONObject()
        .put("key", "keyValue").put("subkey","subkeyValue")
        .put("contents", "Test Contents!");

        // test put
        this.dynamodb.withTable(TEST_TABLE)
            .withKey("key", "keyValue").withSubkey("subkey", "subkeyValue");
        try {
            this.dynamodb.put(record);
        } catch (Exception e) {
            e.printStackTrace();
            fail("fail to test put : "+e.getMessage());
        }

        // test get
        this.dynamodb.withTable(TEST_TABLE)
            .withKey("key", "keyValue").withSubkey("subkey", "subkeyValue");
        try {
            JSONObject result = this.dynamodb.get();
            @SuppressWarnings("unchecked")
            Set<String> keys = result.keySet();
            for(String key:keys){
                assertEquals(result.getString(key),record.getString(key));
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail("fail to test get : "+e.getMessage());
        }

        // test update
        JSONObject updateData = new JSONObject().put("contents", "contents updated!");
        this.dynamodb.withTable(TEST_TABLE)
            .withKey("key", "keyValue").withSubkey("subkey", "subkeyValue");
        try {
            this.dynamodb.update(updateData);
            // confirm
            this.dynamodb.withTable(TEST_TABLE)
                .withKey("key", "keyValue").withSubkey("subkey", "subkeyValue");
            JSONObject result = this.dynamodb.get();
            assertNotNull(result);
            assertEquals(result.getString("contents"),updateData.getString("contents"));
        } catch (Exception e) {
            e.printStackTrace();
            fail("fail to test update : "+e.getMessage());
        }

        // test delete
        this.dynamodb.withTable(TEST_TABLE)
            .withKey("key", "keyValue").withSubkey("subkey", "subkeyValue");
        try {
            this.dynamodb.delete();
            // confirm
            this.dynamodb.withTable(TEST_TABLE)
                .withKey("key", "keyValue").withSubkey("subkey", "subkeyValue");
            JSONObject result = this.dynamodb.get();
            assertNull(result);
        } catch (Exception e) {
            e.printStackTrace();
            fail("fail to test delete : "+e.getMessage());
        }

    }

    /**
     * Test method for {@link com.metaps.util.datastore.Redis#batchGet(org.json.JSONArray)}.
     */
    @Test
    public void testBatch() {

        JSONArray conditions = new JSONArray();
        JSONArray regDatas = new JSONArray();
        JSONArray delConditions = new JSONArray();
        String keyName = "key";
        String subName = "subkey";
        for(int num=1; num<=10; num++){
            String key = "testpkey";
            String subkey = "subkey-"+num;

            JSONObject co = new JSONObject().put(keyName, key).put(subName, subkey);
            JSONObject cond = new JSONObject();
            cond.put("table",TEST_TABLE).put("key", keyName).put("subkey",subName)
            .put("action", "put").put("data", co);
            conditions.put(cond);

            JSONObject record = new JSONObject();
            record.put(keyName, key).put(subName, subkey).put("contents", "contents-"+num);
            JSONObject data = new JSONObject();
             data.put("table",TEST_TABLE).put("key", keyName).put("subkey",subName)
            .put("action", "put").put("data", record);
            regDatas.put(data);

           JSONObject delCond = new JSONObject();
            delCond.put("table",TEST_TABLE).put("key", keyName).put("subkey",subName)
            .put("action", "delete").put("data", co);
            delConditions.put(delCond);
        }

        // test batchWrite
        try {
            this.dynamodb.batchWrite(regDatas);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Fail to exec batchWrite : "+e.getMessage());
        }

        // test batchGet
        try {
            JSONObject result = this.dynamodb.batchGet(conditions);
            System.out.println(result.toString());
            for(int num=0; num<regDatas.length(); num++){
                JSONObject org = regDatas.getJSONObject(num).getJSONObject("data");
                for(int pos=0; pos<result.getJSONArray(TEST_TABLE).length(); pos++){
                    JSONObject row = result.getJSONArray(TEST_TABLE).getJSONObject(num);
                    if(row.getString("key").equalsIgnoreCase(org.getString("key"))
                            && row.getString("subkey").equalsIgnoreCase(org.getString("subkey"))
                            && !row.getString("contents").equalsIgnoreCase(org.getString("contents"))
                            ){
                        fail("contents unmatch.");
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail("Fail to exec batchWrite : "+e.getMessage());
        }

        // full scan
        try {
            ResultSet resultSet = this.dynamodb.withTable(TEST_TABLE)
                    .scan(null, Arrays.asList("key","subkey","contents"));
            int counter = 0;
            while(resultSet.hasNext()){
                JSONObject row = resultSet.next();
                System.out.println(row.toString());
                counter++;
            }
            assertTrue(counter>=conditions.length());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Fail to scan : "+e.getMessage());
        }

        // query
        JSONObject queryCond = new JSONObject();
        queryCond.put("key", new JSONObject().put("value","testpkey").put("operator","="));
        try {
            ResultSet resultSet = this.dynamodb.withTable(TEST_TABLE)
                    .withKey("key", null).withSubkey(null, null)
                    .query(queryCond,Arrays.asList("key","subkey","contents"));
            System.out.println(resultSet.toString());
            int counter = 0;
            while(resultSet.hasNext()){
                counter++;
                JSONObject row = resultSet.next();
                System.out.println(row.toString());
            }
            assertEquals(counter,conditions.length());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Fail to query : "+e.getMessage());
        }

        try {
            // delete all
            this.dynamodb.batchWrite(delConditions);
            JSONObject result = this.dynamodb.batchGet(conditions);
            System.out.println(result.toString());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Fail to exec batchWrite delete : "+e.getMessage());
        }

    }

}
