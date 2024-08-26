import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.net.URL;
import java.util.stream.Collectors;

/**
 * This class will search for process instances that:
 * * exist in Zeebe in specific partitions 
 * * keys are in defined ranges (ranges of lost data)
 * * don't exist in Operate
 *
 * Zeebe data is in form of snapshot downloaded from Zeebe Broker node for specific partitions.
 * 
 * This class will also collect the corresponding variable values (variable name is defined in constant).
 * 
 * The output will be file `process-instances-4-removal` as well as `varNameVars-4-removal`.
 *
 * The class will also store intermediate data in separate files.
 * 
 */
public class SearchForMissingInstances {

  private static final Logger LOGGER = LogManager.getLogger(SearchForMissingInstances.class.getName());
  public static final String DIRECTORY_PATH = "C:\\Users\\SvetlanaDorokhova\\Documents\\operate\\SUPPORT-22102\\cancel\\20240815-zeebe-data\\usr\\local\\zeebe\\data\\";
  public static final String PROCESS_INSTANCES_FILE_PATH = "process-instances";
  public static final String FLOW_NODE_INSTANCES_FILE_PATH = "flow-node-instances.json";
  public static final String FLOW_NODE_INSTANCES_SMALL_FILE_PATH = "flow-node-instances-small.json";
  public static final String VAR_NAME_VARS_FILE_PATH = "varNameVars.json";
  public static final String PROCESS_INSTANCES_4_REMOVAL_FILE_PATH = "process-instances-4-removal";
  public static final String FLOW_NODE_INSTANCES_4_REMOVAL_FILE_PATH = "flow-node-instances-4-removal.json";
  public static final String VAR_NAME_VARS_4_REMOVAL_FILE_PATH = "varNameVars-4-removal.json";
  private static final URL PR_INST_BATCH_FILE_URL = SearchForMissingInstances.class.getResource("pr-inst.bat");
  private static final String ZDB_PATH = "java -jar C:\\programs\\zdb\\zdb.jar";
  private static final Map<Long, Long> LOST_INSTANCES_KEYS = new HashMap<>();

  static {
    LOST_INSTANCES_KEYS.putAll(
        Map.of(78812994103625574L, 78812994150953317L,
            81064793917882551L, 81064793965226886L,
            83316593730761667L, 83316593778317768L,
            85568393546200767L, 85568393602626029L,
            87820193363336055L, 87820193410897193L,
            90071993172277585L, 90071993219787111L,
            92323792985885583L, 92323793042216457L,
            94575592799336956L, 94575592846901442L,
            96827392622659782L, 96827392670183355L,
            99079192426623046L, 99079192474176354L));
    LOST_INSTANCES_KEYS.putAll(
        Map.of(101330992240725619L, 101330992288155839L,
        103582792055011995L, 103582792090054675L,
        105834591868049820L, 105834591903161157L,
        108086391686847593L, 108086391734299745L,
        110338191495794589L, 110338191543347098L,
        112589991308498236L, 112589991355937963L))
    ;
  }

  private List<Long> processInstances4Removal = new ArrayList<>();
  private Map<String, Long> flowNodeInstances4Removal = new HashMap<>();
  private List<Map<String, Object>> varNames4Removal = new ArrayList<>();

  public static void main(String[] args) throws Exception {
    try {
      new SearchForMissingInstances().execute();
    } catch (Exception ex) {
      LOGGER.error(ex.getMessage());
      ex.printStackTrace();
    }
  }

  private void execute() throws Exception {
    try {
      Path dir = Paths.get(DIRECTORY_PATH);

      try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
        for (Path entry : stream) {
          if (Files.isDirectory(entry)) {
            LOGGER.info("Partition: " + entry.getFileName());
            String partitionPath = DIRECTORY_PATH + entry.getFileName() + "\\snapshots\\";
            try (DirectoryStream<Path> stream2 = Files.newDirectoryStream(Paths.get(partitionPath))) {
              for (Path entry2 : stream2) {
                if (Files.isDirectory(entry2)) {
                  LOGGER.info("Snapshot: " + entry2.getFileName());
                  String snapshotPath = partitionPath + entry2.getFileName();
                  // collect all process instance keys from Zeebe data
                  collectProcessInstances(snapshotPath);
                  // collect all variables from Zeebe data
                  collectVariables(snapshotPath);
                  // collect all flow node instances from Zeebe data
                  collectFlowNodeInstances(snapshotPath);
                }
              }
            }
          }
        }
      }
      // filter process instances using known "lost data" key intervals
      filterProcessInstances();
      // filter variables to leave only varName variables
      filterVarNameVariables();
      // minimize flow node instance data for convenience
      convertFlowNodeInstances();
      // select flow node instances related to filtered process instances
      filterFlowNodeInstances();
      // select variables related to filtered process instances
      filterVariables();

    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  private void filterVariables() {
    try {

      ObjectMapper objectMapper = new ObjectMapper();
      //read flow node instances for removal
      if (flowNodeInstances4Removal.isEmpty()) {
        File jsonFile = new File(FLOW_NODE_INSTANCES_4_REMOVAL_FILE_PATH);
        flowNodeInstances4Removal = objectMapper.readValue(jsonFile, new TypeReference<>() {
        });
      }
      File jsonFile = new File(VAR_NAME_VARS_FILE_PATH);
      List<Map<String, Object>> dataList = objectMapper.readValue(jsonFile, new TypeReference<>() {});
      for (Map<String, Object> data : dataList) {
        String scopeKey = ((String) data.get("key")).split(":")[0];
        if (flowNodeInstances4Removal.keySet().contains(scopeKey)) {
          String valueBase64 = (String)((Map)data.get("value")).get("value");
          String value = new String(Base64.getDecoder().decode(valueBase64.getBytes()));
          varNames4Removal.add(Map.of("key", ((Map) data.get("value")).get("key"),
              "processInstanceKey", flowNodeInstances4Removal.get(scopeKey),
              "flowNodeInstanceKey", scopeKey,
              "valueBase64", valueBase64,
              "value", value));
        }
      }
      File outputFile = new File(VAR_NAME_VARS_4_REMOVAL_FILE_PATH);
      objectMapper.writeValue(outputFile, varNames4Removal);
      LOGGER.info("varName vars for removal has been written to the file as JSON.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void filterFlowNodeInstances() {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      //read process instances for removal
      if (processInstances4Removal.isEmpty()) {
        List<String> lines = Files.readAllLines(Paths.get(PROCESS_INSTANCES_4_REMOVAL_FILE_PATH));
        // Convert the List<String> to List<Long>
        processInstances4Removal = lines.stream().map(Long::valueOf).collect(Collectors.toList());
      }
      File jsonFile = new File(FLOW_NODE_INSTANCES_SMALL_FILE_PATH);
//      List<Map<String, Object>> dataList = objectMapper.readValue(jsonFile, new TypeReference<>() {
//      });
      final int batchSize = 500000;
      try (FileInputStream fileInputStream = new FileInputStream(jsonFile);
          JsonParser jsonParser = objectMapper.getFactory().createParser(fileInputStream)) {

        if (jsonParser.nextToken() == JsonToken.START_ARRAY) {
          List<Map<String, Object>> batch = new ArrayList<>();
          while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
            Map<String, Object> record = objectMapper.readValue(jsonParser, new TypeReference<>() {
            });
            batch.add(record);
            if (batch.size() >= batchSize) {
              processBatch(batch);
              batch.clear(); // Clear the batch after processing
            }
          }
          // Process any remaining records in the last batch
          if (!batch.isEmpty()) {
            processBatch(batch);
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }

//      flowNodeInstances4Removal = dataList.stream().parallel()
//          .filter(m -> processInstances4Removal.contains(m.get("processInstanceKey"))).collect(
//              Collectors.toMap(m -> (String) m.get("key"), m -> (Long) m.get("processInstanceKey"),
//                  (existing, replacement) -> existing));
      File outputFile = new File(FLOW_NODE_INSTANCES_4_REMOVAL_FILE_PATH);
      objectMapper.writeValue(outputFile, flowNodeInstances4Removal);
      LOGGER.info("Flow node instances for removal has been written to the file as JSON.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void processBatch(List<Map<String, Object>> batch) {
    flowNodeInstances4Removal.putAll(
        batch.stream().parallel().filter(m -> processInstances4Removal.contains(m.get("processInstanceKey"))).collect(
            Collectors.toMap(m -> (String) m.get("key"), m -> (Long) m.get("processInstanceKey"),
                (existing, replacement) -> existing)));
  }

  private void filterProcessInstances() {
    try {
      processInstances4Removal = Files.lines(Paths.get(PROCESS_INSTANCES_FILE_PATH))
          .map(Long::parseLong)
          .filter(key -> {
            final boolean[] isLost = { false };
            LOST_INSTANCES_KEYS.entrySet()
                .forEach(lost -> isLost[0] = isLost[0] || (lost.getKey() <= key && lost.getValue() >= key));
            return isLost[0];})
          .collect(Collectors.toList());
      Files.write(Paths.get(PROCESS_INSTANCES_4_REMOVAL_FILE_PATH),
          processInstances4Removal.stream().map(String::valueOf).collect(Collectors.toList()));
      LOGGER.info("Process instances for removal has been written to the file as JSON.");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void filterVarNameVariables() throws IOException, InterruptedException {
    String[] command = {"jq", "[.data[] | select(.key | test(\\\"[0-9]+:varName\\\"))]", "variables.json"};
    Process process = Runtime.getRuntime().exec(command);
    File outputFile = new File(VAR_NAME_VARS_FILE_PATH);
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        FileWriter writer = new FileWriter(outputFile, true)) {
      String line;
      while ((line = reader.readLine()) != null) {
        writer.write(line + System.lineSeparator());
      }
    }
    int exitCode = process.waitFor();
    LOGGER.info("Process filterVariables exited with code: " + exitCode);
  }

  private void convertFlowNodeInstances() throws IOException, InterruptedException {
    String[] command = {"jq", "[.data[] | {key: .key, processInstanceKey: .value.elementRecord.processInstanceRecord.processInstanceKey}]", FLOW_NODE_INSTANCES_FILE_PATH, ">>" + FLOW_NODE_INSTANCES_SMALL_FILE_PATH};
    Process process = Runtime.getRuntime().exec(command);
    BufferedReader reader = new BufferedReader(new InputStreamReader( process.getErrorStream()));
    String line;
    while ((line = reader.readLine()) != null) {
      LOGGER.error(line);
    }
//    File outputFile = new File(FLOW_NODE_INSTANCES_SMALL_FILE_PATH);
//    try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
//        FileWriter writer = new FileWriter(outputFile, true)) {
//      String line;
//      while ((line = reader.readLine()) != null) {
//        writer.write(line + System.lineSeparator());
//      }
//    }
    int exitCode = process.waitFor();
    LOGGER.info("Process convertFlowNodeInstances exited with code: " + exitCode);
  }

  private void collectVariables(String snapshotPath) throws IOException, InterruptedException {
    Process process = Runtime.getRuntime()
        .exec(ZDB_PATH + " state list -p=" + snapshotPath + " -cf=VARIABLES -kf=\"ls\"");
    File outputFile = new File("variables.json");
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        FileWriter writer = new FileWriter(outputFile, true)) {
      String line;
      while ((line = reader.readLine()) != null) {
        writer.write(line + System.lineSeparator());
      }
    }
    int exitCode = process.waitFor();
    LOGGER.info("Process collectVariables exited with code: " + exitCode);
  }

  private void collectProcessInstances(String snapshotPath) throws IOException, InterruptedException {
    Process process = Runtime.getRuntime()
        .exec(PR_INST_BATCH_FILE_URL.getPath() + " " + snapshotPath + " " + PROCESS_INSTANCES_FILE_PATH);
    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
    String line;
    while ((line = reader.readLine()) != null) {
      LOGGER.info(line);
    }
    process.waitFor();
  }

  private void collectFlowNodeInstances(String snapshotPath) throws IOException, InterruptedException {
    Process process = Runtime.getRuntime()
        .exec(ZDB_PATH + " state list -p=" + snapshotPath + " -cf=ELEMENT_INSTANCE_KEY -kf=\"l\"");
    File outputFile = new File(FLOW_NODE_INSTANCES_FILE_PATH);
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        FileWriter writer = new FileWriter(outputFile, true)) {
      String line;
      while ((line = reader.readLine()) != null) {
        writer.write(line + System.lineSeparator());
      }
    }
    int exitCode = process.waitFor();
    LOGGER.info("Process collectFlowNodeInstances exited with code: " + exitCode);
  }

}
