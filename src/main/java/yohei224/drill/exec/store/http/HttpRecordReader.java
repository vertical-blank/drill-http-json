package yohei224.drill.exec.store.http;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.easy.json.JsonProcessor;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import yohei224.drill.exec.store.http.HttpSubScan.HttpSubScanSpec;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;

import java.io.IOException;
import java.util.*;

import static sun.net.www.protocol.http.HttpURLConnection.userAgent;

public class HttpRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HttpRecordReader.class);
  private static final long DEFAULT_ROWS_PER_BATCH = 4096L;

  private final HttpSubScan.HttpSubScanSpec scanSpec;
  private JsonReader jsonReader;
  private VectorContainerWriter writer;
  private FragmentContext fragmentContext;
  private boolean enableAllTextMode = false;
  private boolean readNumbersAsDouble = false;
  private int recordCount;
  private long runningRecordCount;
  private Collection<SchemaPath> columns;

  public HttpRecordReader(HttpSubScanSpec subScanSpec, List<SchemaPath> columns, FragmentContext context) {
    scanSpec = subScanSpec;
    this.columns = columns;
    fragmentContext = context;
    logger.debug("Scan spec: {}", subScanSpec);
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    this.writer = new VectorContainerWriter(output, false);
    this.jsonReader = new JsonReader(this.fragmentContext.getManagedBuffer(), ImmutableList.copyOf(this.getColumns()), this.enableAllTextMode, true, this.readNumbersAsDouble);

    String tableName = scanSpec.getTableName();

    HttpClient client = new DefaultHttpClient();
    HttpGet httpGet = new HttpGet(tableName);
    try {
      HttpResponse response = client.execute(httpGet);
      int responseStatus = response.getStatusLine().getStatusCode();
      String body = EntityUtils.toString(response.getEntity(), "UTF-8");

      //JsonNode jsonNode = new ObjectMapper().convertValue(body, JsonNode.class);

      this.jsonReader.setSource(body.getBytes(Charsets.UTF_8));
    } catch (IOException e) {
      throw new ExecutionSetupException(e);
    }

//    // TODO get data from url
//    Map<String, Object> map1 = new HashMap<>();
//    map1.put("hoge", 11);
//    map1.put("fuga", 12);
//    map1.put("piyo", "piyo");
//    map1.put("tbl", tableName);
//    Map<String, Object> map2 = new HashMap<>();
//    map2.put("hoge", 21);
//    map2.put("fuga", 22);
//    map2.put("piyo", "piyopiyo");
//    map2.put("tbl", tableName);
//    JsonNode jsonNode = new ObjectMapper().convertValue(Arrays.asList(map1, map2), JsonNode.class);
//    this.jsonReader.setSource(jsonNode);
  }

  @Override
  protected Collection<SchemaPath> getColumns() {
    return this.columns;
  }

  @Override
  public int next() {
    this.writer.allocate();
    this.writer.reset();

    this.recordCount = 0;
    JsonProcessor.ReadState write = null;

    try {
      while((long)this.recordCount < DEFAULT_ROWS_PER_BATCH) {
        this.writer.setPosition(this.recordCount);

        write = this.jsonReader.write(this.writer);
        if(write != JsonProcessor.ReadState.WRITE_SUCCEED) {
          break;
        }

        ++this.recordCount;
      }

      this.jsonReader.ensureAtLeastOneField(this.writer);
      this.writer.setValueCount(this.recordCount);
      this.updateRunningCount();
      return this.recordCount;

    } catch (Exception var3) {
      this.handleAndRaise("Error parsing JSON", var3);
      return 0;
    }
  }

  @Override
  public void close() {
  }

  protected void handleAndRaise(String suffix, Exception e) throws UserException {

    String message = e.getMessage();
    int columnNr = -1;

    if (e instanceof JsonParseException) {
      final JsonParseException ex = (JsonParseException) e;
      message = ex.getOriginalMessage();
      columnNr = ex.getLocation().getColumnNr();
    }

    UserException.Builder exceptionBuilder = UserException.dataReadError(e)
            .message("%s - %s", suffix, message);
    if (columnNr > 0) {
      exceptionBuilder.pushContext("Column ", columnNr);
    }

    throw exceptionBuilder.build(logger);
  }

  private void updateRunningCount() {
    this.runningRecordCount += (long)this.recordCount;
  }

}
