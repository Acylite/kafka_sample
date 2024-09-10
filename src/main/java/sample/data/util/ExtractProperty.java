package sample.data.util;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.util.Utf8;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.JSONException;
import com.google.gson.*;


import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.databind.type.MapType;

import com.fasterxml.jackson.core.type.TypeReference;

public class ExtractProperty {
  
  public static <T> T safeCast(GenericRecord record, String propertyName, Class<T> clazz) {
    if (record == null) {
      return null;
    }
    Object o = record.get(propertyName);
    return clazz != null && clazz.isInstance(o) ? clazz.cast(o) : null;
  }

  public static Long safeCastLong(GenericRecord record, String propertyName) {
    if (record == null) {
      return null;
    }
    String o = record.get(propertyName).toString();
    long l = Long.parseLong(o);
    return o.length() == 13 ? l/1000 : l;
  }

  public static <T> T safeCast(String record, String propertyName, Class<T> clazz) {
    if (record == null) {
      return null;
    }
    try {
      // Create Gson instance
      Gson gson = new Gson();

      // Parse JSON string to JsonObject
      JsonObject jsonObject = gson.fromJson(record, JsonObject.class);

      if (jsonObject.has(propertyName)) {
        // Accessing values from JsonObject
        JsonElement o = jsonObject.get(propertyName);
        if (o != null && !o.isJsonNull()) {
          // Check if property is not null
          return gson.fromJson(o, clazz);
        }
      } else {
        return null;
      }
    } catch (Exception e) {
        e.printStackTrace();
    }
    return null;
  }

  public static String safeCastUtf8String(String record, String propertyName) {
    if (record == null) {
      return null;
    }
    try {
      // Create Gson instance
      Gson gson = new Gson();
    
      // Parse JSON string to JsonObject
      JsonObject jsonObject = gson.fromJson(record, JsonObject.class);
      if (jsonObject.has(propertyName) ) {
        // Accessing values from JsonObject
        JsonElement o = jsonObject.get(propertyName);
        if(o != null && !o.isJsonNull()){
          return o.getAsString();
        } 
      }
      return null;
    } catch (Exception e) {
        e.printStackTrace();
    }
    return null;
  }

  public static String safeCastUtf8String(GenericRecord record, String propertyName) {
    if(record == null){
      return null;
    }
    if (record != null) {
      Object o = record.get(propertyName);
      if (o != null) {
        if (o instanceof Utf8) {
          return ((Utf8) o).toString();
        }
      }
    }
    return null;
  }

  public static <T> ArrayList<T> safeCastArray(GenericRecord record, String propertyName,
      Class<T> clazz) {
    if (record != null) {
      Array<Object> arr = (Array<Object>) record.get(propertyName);
      if (arr != null) {
        ArrayList<T> values = new ArrayList<>();
        for (Object o : arr) {
          if (clazz.isInstance(o)) {
            values.add(clazz.cast(o));
          }
        }
        return values;
      }
    }
    return new ArrayList<>();
  }

  public static <T> ArrayList<T> safeCastArray(String record, String propertyName,
      Class<T> clazz) {
      if(record != null && record != ""){
        JSONObject jsonObject = new JSONObject(record);
        if (jsonObject.optJSONArray(propertyName) != null) {
          JSONArray arr = (JSONArray) jsonObject.optJSONArray(propertyName);
          if (arr != null) {
            ArrayList<T> values = new ArrayList<>();
            for (Object o : arr) {
              if (clazz.isInstance(o)) {
                values.add(clazz.cast(o));
              }
            }
            return values;
          }
        }
      }
    return new ArrayList<>();
  }

  public static String safeCastJSONString(String record, String propertyName){
    if(record != "" && record != null){
      JSONObject jsonObject = new JSONObject(record);
      if (jsonObject.optString(propertyName) != null) {
          return jsonObject.optString(propertyName);
      }
    }
    return null;  
    // Extract a simple string
  }



  public static <T> ArrayList<String> safeCastDTOArray(String record, String propertyName) {
      if(record != null && record != ""){

        ArrayList<String> values = new ArrayList<>();
        JSONObject jsonObject = new JSONObject(record);
       
        if(jsonObject.optJSONArray(propertyName) != null){
          JSONArray arr = jsonObject.optJSONArray(propertyName);
          for (int i = 0; i < arr.length(); i++) {
              String temp = arr.get(i).toString();
              values.add(temp);
            }
          return values;
        }
       
        
      }
    return new ArrayList<>();
  }

  public static ArrayList<String> safeCastUtf8StringArray(GenericRecord record,
      String propertyName) {
    if (record != null) {
      Array<Utf8> arr = (Array<Utf8>) record.get(propertyName);
      if (arr != null) {
        ArrayList<String> stringArr = new ArrayList<>();
        for (Utf8 ele : arr) {
          stringArr.add(ele.toString());
        }
        return stringArr;
      }
    }
    return null;
  }
  
  public static List<String> safeCastUtf8StringList(GenericRecord record, String propertyName){
    if (record == null) {
      return null;
    }
    try {
    
      // Parse JSON string to JsonObjectrecord
      JSONObject jsonObject = new JSONObject(record);
      if(jsonObject.optJSONArray(propertyName) != null){
        JSONArray stringArray = jsonObject.getJSONArray(propertyName);
        if (jsonObject.has(propertyName)) {
          // Accessing values from JsonObject
          List<String> stringList = new ArrayList<>();
          for (int i = 0; i < stringArray.length(); i++) {
              stringList.add(stringArray.getString(i));
          }
          return stringList; 
        }
      };
      
    } catch (JSONException e) {
        e.printStackTrace();
    }
    return null;
  }


  public static List<String> safeCastUtf8StringList(String record, String propertyName) {
    if (record == null || record == "") {
      return null;
    }
    try {
    
      // Parse JSON string to JsonObject
      JSONObject jsonObject = new JSONObject(record);
      if(jsonObject.optJSONArray(propertyName) != null){
        JSONArray stringArray = jsonObject.getJSONArray(propertyName);
        if (jsonObject.has(propertyName)) {
          // Accessing values from JsonObject
          List<String> stringList = new ArrayList<>();
          for (int i = 0; i < stringArray.length(); i++) {
              stringList.add(stringArray.getString(i));
          }
          return stringList; 
        }
      };
      
    } catch (JSONException e) {
        e.printStackTrace();
    }
    return null;
  }


  public static List<Integer> safeCastUtf8IntList(String record, String propertyName) {
    if (record == null || record == "") {
      return null;
    }
    try {
      // Parse JSON string to JsonObject
      JSONObject jsonObject = new JSONObject(record);
      
      if(jsonObject.has(propertyName)){
        Object propertyValue = jsonObject.get(propertyName);
        
        if(propertyValue instanceof JSONArray) {
          JSONArray stringArray = (JSONArray) propertyValue;
          // Accessing values from JsonObject
          List<Integer> stringList = new ArrayList<>();
          for (int i = 0; i < stringArray.length(); i++) {
              if(stringArray.get(i) instanceof String){
                String strElement = stringArray.getString(i);
                Integer intElement =  Integer.parseInt(strElement);
                stringList.add(intElement);
              }
              else if(stringArray.get(i) instanceof Integer){
                Integer intElement =  stringArray.getInt(i);
                stringList.add(intElement);
              }
          }
          return stringList;
        }
        if (propertyValue instanceof String) {
          String stringValue = (String) propertyValue;
          try {
              Integer integerValue = Integer.parseInt(stringValue);
              List<Integer> integerList = new ArrayList<>();
              integerList.add(integerValue);
              return integerList;
          } catch (NumberFormatException e) {
              return null;
          }
        }
        if (propertyValue instanceof Integer) {
          Integer integerValue = (Integer) propertyValue;
          List<Integer> integerList = new ArrayList<>();
          integerList.add(integerValue);
          return integerList;
        }
      }
    } catch (JSONException e) {
        e.printStackTrace();
    }
    return null;
  }

  public static String safeCastEnumString(GenericRecord record, String propertyName) {
    if (record != null) {
      Object o = record.get(propertyName);
      if (o instanceof EnumSymbol) {
        return o.toString();
      }
    }
    return null;
  }

  public static Date safeCastDate(GenericRecord record, String propertyName) {
    if (record != null) {
      Object o = record.get(propertyName);
      if (o instanceof Long) {
        return new Date((Long) o);
      }
      if (o instanceof String) {
        try {
          return DateFormat.getDateInstance().parse(o.toString());
        } catch (ParseException e) {
          e.printStackTrace();
        }
      }
    }
    return null;
  }

  //return DateFormat.getDateInstance().parse(s);

  public static Date safeCastDateDivide1000(GenericRecord record, String propertyName) {
    if (record != null) {
      Object o = record.get(propertyName);
      if (o instanceof Long) {
        return new Date((Long) o / 1000);
      }
    }
    return null;
  }

  public static Date longToDate(Long l) {
    if (l != null) {
      return new Date(l);
    }
    return null;
  }

  public static Date longToDateDivide1000(Long l) {
    if (l != null) {
      return new Date(l / 1000);
    }
    return null;
  }
}
