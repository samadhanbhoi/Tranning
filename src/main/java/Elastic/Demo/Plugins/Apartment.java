package Elastic.Demo.Plugins;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.search.SortField;
import org.apache.lucene.util.MathUtil;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.support.RestUtils;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Apartment extends BaseRestHandler {
  
  ObjectMapper mapper = new ObjectMapper();
  
  @Inject
  protected Apartment(Settings settings, RestController controller, Client client)
  {
    super(settings, controller, client);
    controller.registerHandler(Method.POST, "/create/*/*", this);
    controller.registerHandler(Method.GET, "/searchbyid/*/*", this);
    controller.registerHandler(Method.POST, "/search/*/*", this);
    controller.registerHandler(Method.GET, "/search/*/*", this);
  }
  
  @Override
  protected void handleRequest(RestRequest request, RestChannel channel, Client client)
      throws Exception
  {
    mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
    String[] params = request.path()
        .split("/");
    String op = params[1];
    String indexName = params[2].toLowerCase();
    String type = params[3].toLowerCase();
    String id = "";
    System.out.println("op :" + op + "indexName : " + indexName + " Type :" + type);
    Map<String, Object> postData = new HashMap<String, Object>();
    if (request.hasContent()) {
      BytesReference postBody = request.content();
      postData = mapper.readValue(postBody.streamInput(), new TypeReference<Map<String, Object>>()
      {
      });
    }
    String responseString = "sucess", SortField = "";
    int page = 1, from = 0, size = 5;
    if ("create".equalsIgnoreCase(op)) {
      if (params.length > 5) {
        id = params[4].toLowerCase();
      }
      responseString = CommonUtils.createIndex(client, indexName, type, id, postData);
      channel.sendResponse(new BytesRestResponse(RestStatus.OK, responseString));
    }
    else if ("search".equalsIgnoreCase(op)) {
      if (request.hasParam("field")) {
        SortField = request.param("field");
      }
      if (request.hasParam("page")) {
        page = Integer.parseInt(request.param("page"));
      }
      if (request.hasParam("size")) {
        size = Integer.parseInt(request.param("size"));
      }
      from = Math.abs(size * (page - 1));
      responseString = CommonUtils.search(client, indexName, type, from, size, SortField, postData);
      channel.sendResponse(new BytesRestResponse(RestStatus.OK, responseString));
    }
    else {
      GetRequest getRequest = new GetRequest(indexName).type(type);
      if (!id.isEmpty()) {
        getRequest.id(id);
      }
      responseString = client.get(getRequest)
          .actionGet()
          .getSourceAsString();
      channel.sendResponse(new BytesRestResponse(RestStatus.OK, responseString));
    }
    
  }
}
