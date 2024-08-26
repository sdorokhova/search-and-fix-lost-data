
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

/**
 * This class will send cancel commands to Zeebe for process instance keys defined in file.
 */
public class CancelMissingInstancesInZeebe {

  private static final Logger LOGGER = LogManager.getLogger(CancelMissingInstancesInZeebe.class.getName());

  public static void main(String[] args) throws Exception {
    try {
      new CancelMissingInstancesInZeebe().execute();
    } catch (Exception ex) {
      LOGGER.error(ex.getMessage());
      ex.printStackTrace();
    }
  }

  private void execute() throws IOException {
    //read list of process instance keys
    List<Long> processInstances4Removal
        = Files.lines(Paths.get(SearchForMissingInstances.PROCESS_INSTANCES_4_REMOVAL_FILE_PATH))
        .map(Long::parseLong).collect(Collectors.toList());

    ForkJoinPool customThreadPool = new ForkJoinPool(10);
    int maxRetries = 3;

    customThreadPool.submit(() ->
        processInstances4Removal.parallelStream().forEach(key -> {
          boolean success = false;
          int attempt = 0;
          while (!success && attempt < maxRetries) {
            attempt++;
            try {
              cancelProcessInstance(key);
              LOGGER.info(key + ": CANCELLED");
              success = true;
            } catch (Exception e) {
              if (attempt >= maxRetries) {
                LOGGER.error(key + ": FAILED after " + maxRetries + " attempts. Moving on...");
              } else {
                LOGGER.error(key + ": FAILED with " + e.getMessage() + " Retrying...", e);
              }
            }
          }
        })
    ).join();

    customThreadPool.shutdown();
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
