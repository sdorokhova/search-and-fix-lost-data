/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a proprietary license.
 * See the License.txt file for more information. You may not use this file
 * except in compliance with the proprietary license.
 */

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestHighLevelClientBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class ElasticsearchConnector {

  private static final Logger LOGGER = LogManager.getLogger(ElasticsearchConnector.class.getName());

  public static void closeEsClient(RestHighLevelClient esClient) {
    if (esClient != null) {
      try {
        esClient.close();
      } catch (IOException e) {
        LOGGER.error("Could not close esClient",e);
      }
    }
  }

  public static RestHighLevelClient createEsClient(String url, String user, String password) {
    LOGGER.debug("Creating Elasticsearch connection...");
    final RestClientBuilder restClientBuilder =
        RestClient.builder(getHttpHost(url))
            .setHttpClientConfigCallback(
                httpClientBuilder -> setupAuthentication(httpClientBuilder, user, password));
    final RestHighLevelClient esClient = new RestHighLevelClientBuilder(restClientBuilder.build())
        .setApiCompatibilityMode(true).build();
    return esClient;
  }

  private static HttpHost getHttpHost(String url) {
    try {
      final URI uri = new URI(url);
      return new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
    } catch (URISyntaxException e) {
      throw new RuntimeException("Error in url: " + url, e);
    }
  }

  private static HttpAsyncClientBuilder setupAuthentication(
      final HttpAsyncClientBuilder builder, String user, String password) {
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(
        AuthScope.ANY, new UsernamePasswordCredentials(user, password));
    builder.setDefaultCredentialsProvider(credentialsProvider);
    return builder;
  }
}
