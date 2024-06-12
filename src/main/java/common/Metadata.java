package common;

import method.Index;

import java.util.HashMap;
import java.util.Map;

public class Metadata {
    private static HashMap<String, EventSchema> schemaMap;
    private static HashMap<String, Index> indexMap;
    public static Metadata meta = new Metadata();
    private Metadata(){
        schemaMap = new HashMap<>();
        indexMap = new HashMap<>();
    }

    public static Metadata getInstance(){
        return meta;
    }

    public EventSchema getEventSchema(String schemaName){
        return schemaMap.get(schemaName);
    }

    public boolean storeSchema(EventSchema s){
        String schemaName = s.getSchemaName();
        if(schemaMap.containsKey(schemaName)){
            return false;
        }else{
            schemaMap.put(schemaName, s);
            return true;
        }
    }

    public Index getIndex(String schemaName){
        return indexMap.get(schemaName);
    }

    public boolean bindIndex(String schemaName, Index index){
        if(indexMap.containsKey(schemaName)){
            return false;
        }else{
            indexMap.put(schemaName, index);
            return true;
        }
    }

    public void printSchemaMap(){
        System.out.println("Schema map: ");
        for(Map.Entry entry : schemaMap.entrySet()){
            String mapKey = (String) entry.getKey();
            System.out.println("Key: '" + mapKey+"'");
        }
    }
}
