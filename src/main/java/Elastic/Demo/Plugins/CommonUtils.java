package Elastic.Demo.Plugins;

import java.util.Iterator;
import java.util.Map;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
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
    Iterator iterator = postData.keySet()
        .iterator();
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    // Prepare count request
    SearchRequestBuilder searchRequest = client.prepareSearch(indexName)
        .setTypes(type)
        // .setFetchSource(false)
        // .setSearchType(SearchType.QUERY_AND_FETCH);
        .setFrom(0)
        .setSize(5);
    QueryBuilder qb =null;
    if (!sortField.isEmpty()) {
      searchRequest.addSort(sortField, SortOrder.DESC);
    }
    if (iterator.hasNext()) {
      String key = (String) iterator.next();
      Object value = postData.get(key);
      
      qb = QueryBuilders.termQuery(key, value);
    }
    else {
      qb = QueryBuilders.matchAllQuery();
    }
    responseString = mapper.writeValueAsString(searchRequest.setQuery(qb).get()
//      .execute()
//      .actionGet()
      .getHits());
    return responseString;
  }
  
}
