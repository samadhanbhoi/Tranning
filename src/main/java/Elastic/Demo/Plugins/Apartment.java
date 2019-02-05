package Elastic.Demo.Plugins;

import java.util.Map;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Apartment extends BaseRestHandler {
  
  ObjectMapper mapper = new ObjectMapper();
  
  @Inject
  protected Apartment(Settings settings, RestController controller, Client client)
  {
    super(settings, controller, client);
    controller.registerHandler(Method.POST, "/*/*/*", this);
  }
  
  @Override
  protected void handleRequest(RestRequest request, RestChannel channel, Client client)
      throws Exception
  {
    String[] params = request.path().split("/");
    String Op = params[1];
    String indexName = params[2].toLowerCase();
    String type = params[3].toLowerCase();
    System.out.println("indexName : " + indexName + " Type :" + type);
    
    BytesReference postBody = request.content();
    Map<String, Object> postData = mapper.readValue(postBody.streamInput(),
        new TypeReference<Map<String, Object>>(){});
    IndexResponse response = client.prepareIndex(indexName, type)
        .setSource(postData)
        .get();
    System.out.println("endpoint created!!!");
    channel
    .sendResponse(new BytesRestResponse(RestStatus.OK,response.isCreated()+""));
  }
}
