package Elastic.Demo.Plugins;

import java.util.Iterator;
import java.util.Map;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CommonUtils {
  
  static ObjectMapper mapper = new ObjectMapper();
  
  public static String createIndex(Client client, String indexName, String type, String id,
      Map<String, Object> postData)
  {
    IndexResponse response = null;
    if (!id.isEmpty()) {
      response = client.prepareIndex(indexName, type, id)
          .setSource(postData)
          .get();
    }
    else {
      response = client.prepareIndex(indexName, type)
          .setSource(postData)
          .get();
    }
    return response.isCreated() + "";
  }
  
  public static String search(Client client, String indexName, String type, int from,
      int size, String sortField, Map<String, Object> postData) throws JsonProcessingException
  {
    mapper.setVisibility(PropertyAccessor.FIELD,Visibility.ANY);
    String responseString = "";
    Iterator<String> iterator = postData.keySet()
        .iterator();
    SearchRequestBuilder searchRequest = client.prepareSearch(indexName)
        .setTypes(type)
        .setFrom(from)
        .setSize(size);
    QueryBuilder qb =null;
    if (!sortField.isEmpty()) {
      searchRequest.addSort(sortField, SortOrder.ASC);
    }
    if (iterator.hasNext()) {
      String key = (String) iterator.next();
      Object value = postData.get(key);
      
      qb = QueryBuilders.termQuery(key, value);
    }
    else {
      qb = QueryBuilders.matchAllQuery();
    }
   SearchResponse searchResponse = searchRequest.setQuery(qb).setFetchSource("_source", "name").get();
    responseString = searchResponse.toString();//mapper.writeValueAsString(searchResponse);
    return responseString;
  }
  
}
