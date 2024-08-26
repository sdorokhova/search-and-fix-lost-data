
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * This class will search for process instances that exist in Operate but does not exist in Zeebe.
 * It will use the `process-instances` file generated in class SearchForMissingInstances as source of Zeebe data.
 * It will then iterate through old process instances from Operate to check whether they are present in Zeebe data.
 * This class will also cancel found instances in Operate.
 */
public class SearchAndCancelFinishedInstances {

  private static final Logger LOGGER = LogManager.getLogger(SearchAndCancelFinishedInstances.class.getName());
  private static final String PROCESS_INSTANCES_2_CANCEL_IN_OPERATE_FILE_PATH = "process-instances-2-cancel-in-operate";
  public static final String OPERATE_LIST_VIEW_MAIN = "operate-list-view-8.3.0_";
  public static final String OPERATE_FLOW_NODE_INSTANCES_MAIN = "operate-flownode-instance-8.3.1_";
  public static final String OPERATE_INCIDENT_MAIN = "operate-incident-8.3.1_";
  public static final String OPERATE_POST_IMPORTER_QUEUE_MAIN = "operate-post-importer-queue-8.3.0_";
  private static final String END_DATE = "2024-08-26T12:00:00.000+0000";
  private static final String ELASTIC_URL = "http://localhost:9201";
  private static final String ES_USERNAME = "user-rw";
  private static final String ES_PASSWORD = "<...>";
  private static final int BATCH_SIZE = 1000;
  private RestHighLevelClient esClient;
  private List<Long> keysForCancellation  = new ArrayList<>();

  public static void main(String[] args) throws Exception {
    try {
      new SearchAndCancelFinishedInstances().execute();
    } catch (Exception ex) {
      LOGGER.error(ex.getMessage());
      ex.printStackTrace();
    }
  }

  private void execute() throws IOException {
    createEsClient();

    selectKeys2Cancel();

    markInstancesAsCancelled();

    closeEsClient();
  }

  private void markInstancesAsCancelled() throws IOException {
    if (keysForCancellation.isEmpty()) {
      List<String> lines = Files.readAllLines(Paths.get(PROCESS_INSTANCES_2_CANCEL_IN_OPERATE_FILE_PATH));
      keysForCancellation = lines.stream().map(Long::valueOf).collect(Collectors.toList());
    }

    // https://confluence.camunda.com/display/HAN/Cancel+process+instances

    // iterate through process instance keys in batches
    int batchSize = BATCH_SIZE;

    IntStream.range(0, (keysForCancellation.size() + batchSize - 1) / batchSize)
        .mapToObj(i -> keysForCancellation.subList(i * batchSize, Math.min(keysForCancellation.size(), (i + 1) * batchSize)))
        .forEach(keys -> {
          LOGGER.info("Processing process instances with keys: " + keys);
          cancelProcessInstancesBatch(keys);
          cancelFlowNodeInstancesInListView(keys);
          cancelFlowNodeInstances(keys);
          resolveIncidents(keys);
          deletePostImporterQueue(keys);
        });
  }

  private void cancelProcessInstancesBatch(List<Long> keys) {
    try {
      // update process instances in list-view
      UpdateByQueryRequest request = new UpdateByQueryRequest(OPERATE_LIST_VIEW_MAIN).setQuery(
          joinWithAnd(termQuery("joinRelation", "processInstance"), termsQuery("processInstanceKey", keys)));
      request.setScript(new Script(ScriptType.INLINE, "painless",
          "ctx._source.state = 'CANCELED'; ctx._source.incident = false; ctx._source.endDate = '" + END_DATE + "';",
          Collections.emptyMap()));
      request.setConflicts("proceed");

      BulkByScrollResponse bulkResponse = esClient.updateByQuery(request, RequestOptions.DEFAULT);
      long updated = bulkResponse.getUpdated();
      if (updated < keys.size()) {
        LOGGER.warn("Not all process instances were updated. Keys: " + keys);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void cancelFlowNodeInstancesInListView(List<Long> keys) {
    try {
      UpdateByQueryRequest request = new UpdateByQueryRequest(OPERATE_LIST_VIEW_MAIN).setQuery(
          joinWithAnd(termQuery("joinRelation", "activity"),
              termQuery("activityState", "ACTIVE"),
              termsQuery("processInstanceKey", keys)));
      request.setScript(new Script(ScriptType.INLINE, "painless",
          "ctx._source.activityState = 'TERMINATED'; ctx._source.incident = false; ctx._source.endDate = '" + END_DATE + "';",
          Collections.emptyMap()));
      request.setConflicts("proceed");

      BulkByScrollResponse bulkResponse = esClient.updateByQuery(request, RequestOptions.DEFAULT);
      long updated = bulkResponse.getUpdated();
      if (updated < keys.size()) {
        LOGGER.warn("Not all flow node instances in list view were updated. Keys: " + keys);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void cancelFlowNodeInstances(List<Long> keys) {
    try {
      UpdateByQueryRequest request = new UpdateByQueryRequest(OPERATE_FLOW_NODE_INSTANCES_MAIN).setQuery(
          joinWithAnd(termQuery("state", "ACTIVE"),
              termsQuery("processInstanceKey", keys)));
      request.setScript(new Script(ScriptType.INLINE, "painless",
          "ctx._source.state = 'TERMINATED'; ctx._source.incident = false; ctx._source.endDate = '" + END_DATE + "';",
          Collections.emptyMap()));
      request.setConflicts("proceed");

      BulkByScrollResponse bulkResponse = esClient.updateByQuery(request, RequestOptions.DEFAULT);
      long updated = bulkResponse.getUpdated();
      if (updated < keys.size()) {
        LOGGER.warn("Not all flow node instances were updated. Keys: " + keys);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void resolveIncidents(List<Long> keys) {
    try {
      UpdateByQueryRequest request = new UpdateByQueryRequest(OPERATE_INCIDENT_MAIN).setQuery(
          joinWithAnd(termQuery("state", "ACTIVE"),
              termsQuery("processInstanceKey", keys)));
      request.setScript(new Script(ScriptType.INLINE, "painless",
          "ctx._source.state = 'RESOLVED';",
          Collections.emptyMap()));
      request.setConflicts("proceed");

      BulkByScrollResponse bulkResponse = esClient.updateByQuery(request, RequestOptions.DEFAULT);
      long updated = bulkResponse.getUpdated();
      if (updated > 0) {
        LOGGER.info(updated + " incident are resolved.");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void deletePostImporterQueue(List<Long> keys) {
    try {
      DeleteByQueryRequest request = new DeleteByQueryRequest(OPERATE_POST_IMPORTER_QUEUE_MAIN).setQuery(
          joinWithAnd(termsQuery("processInstanceKey", keys)));
      request.setConflicts("proceed");
      BulkByScrollResponse bulkResponse = esClient.deleteByQuery(request, RequestOptions.DEFAULT);
      long deleted = bulkResponse.getDeleted();
      if (deleted > 0) {
        LOGGER.info(deleted + " post importer queue records are removed.");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void selectKeys2Cancel() throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    Set<Long> zeebePIs = Files.lines(Paths.get(SearchForMissingInstances.PROCESS_INSTANCES_FILE_PATH)).map(Long::parseLong)
        .collect(Collectors.toSet());
    final SearchRequest searchRequest =
        new SearchRequest(OPERATE_LIST_VIEW_MAIN)
            .source(
                new SearchSourceBuilder()
                    .query(joinWithAnd(termQuery("joinRelation", "processInstance"),
                        termQuery("state", "ACTIVE"),
                        rangeQuery("startDate").lt("now-3M"),
                        rangeQuery("partitionId").gte(35)))
                    .fetchSource(false)
                    .size(1000)
                    .sort("key", SortOrder.ASC));

    scroll(searchRequest, esClient, sh -> {
      List<Long> keys = map(Arrays.stream(sh.getHits()), (hit) -> Long.valueOf(hit.getId()));
      keys.removeAll(zeebePIs);
      LOGGER.info("Keys for cancellation: " + keys);
      keysForCancellation.addAll(keys);
    });
    File outputFile = new File(PROCESS_INSTANCES_2_CANCEL_IN_OPERATE_FILE_PATH + ".json");
    objectMapper.writeValue(outputFile, keysForCancellation);
    Files.write(Paths.get(PROCESS_INSTANCES_2_CANCEL_IN_OPERATE_FILE_PATH),
        keysForCancellation.stream().map(String::valueOf).collect(Collectors.toList()));
    LOGGER.info("Process instances for cancellation has been written to the file as JSON.");
  }

  private void closeEsClient() {
    ElasticsearchConnector.closeEsClient(esClient);
  }

  private RestHighLevelClient createEsClient() {
    esClient = ElasticsearchConnector.createEsClient(ELASTIC_URL, ES_USERNAME, ES_PASSWORD);
    return esClient;
  }

  public <T> List<T> scroll(
      SearchRequest searchRequest,
      RestHighLevelClient esClient,
      Consumer<SearchHits> searchHitsProcessor)
      throws IOException {

    searchRequest.scroll(TimeValue.timeValueMillis(60000));
    SearchResponse response = esClient.search(searchRequest, RequestOptions.DEFAULT);

    final List<T> result = new ArrayList<>();
    String scrollId = response.getScrollId();
    SearchHits hits = response.getHits();

    while (hits.getHits().length != 0) {

      // call response processor
      if (searchHitsProcessor != null) {
        searchHitsProcessor.accept(response.getHits());
      }

      final SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
      scrollRequest.scroll(TimeValue.timeValueMillis(60000));

      response = esClient.scroll(scrollRequest, RequestOptions.DEFAULT);

      scrollId = response.getScrollId();
      hits = response.getHits();
    }

    clearScroll(scrollId, esClient);

    return result;
  }

  public void clearScroll(String scrollId, RestHighLevelClient esClient) {
    if (scrollId != null) {
      // clear the scroll
      final ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
      clearScrollRequest.addScrollId(scrollId);
      try {
        esClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
      } catch (Exception e) {
        LOGGER.warn("Error occurred when clearing the scroll with id [{}]", scrollId);
      }
    }
  }

  public static <S, T> List<T> map(S[] sourceArray, Function<S, T> mapper) {
    return map(Arrays.stream(sourceArray).parallel(), mapper);
  }

  public static <S, T> List<T> map(Stream<S> sequenceStream, Function<? super S, T> mapper) {
    return sequenceStream.map(mapper).collect(Collectors.toList());
  }

  public QueryBuilder joinWithAnd(QueryBuilder... queries) {
    final List<QueryBuilder> notNullQueries = throwAwayNullElements(queries);
    switch (notNullQueries.size()) {
    case 0:
      return null;
    case 1:
      return notNullQueries.get(0);
    default:
      final BoolQueryBuilder boolQ = boolQuery();
      for (QueryBuilder query : notNullQueries) {
        boolQ.must(query);
      }
      return boolQ;
    }
  }

  public <T> List<T> throwAwayNullElements(T... array) {
    final List<T> listOfNotNulls = new ArrayList<>();
    for (T o : array) {
      if (o != null) {
        listOfNotNulls.add(o);
      }
    }
    return listOfNotNulls;
  }

  private static void cancelProcessInstance(Long key) throws IOException, InterruptedException {
    Process process = Runtime.getRuntime().exec("C:/programs/zbctl-8.3.4/zbctl.exe --insecure cancel instance " + key);
    InputStream errorStream = process.getErrorStream();
    BufferedReader reader = new BufferedReader(new InputStreamReader(errorStream));
    String line;
    while ((line = reader.readLine()) != null) {
      LOGGER.error(line);
    }
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new RuntimeException("Process exited with error " + exitCode + " for " + key);
    }
  }

}
