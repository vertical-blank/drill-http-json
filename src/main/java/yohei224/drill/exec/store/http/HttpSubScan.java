package yohei224.drill.exec.store.http;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.store.StoragePluginRegistry;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

// Class containing information for reading a single Kudu tablet
@JsonTypeName("http-sub-scan")
public class HttpSubScan extends AbstractBase implements SubScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HttpSubScan.class);

  @JsonProperty
  public final HttpStoragePluginConfig storage;


  private final HttpStoragePlugin httpStoragePlugin;
  private final HttpSubScanSpec scanSpec;
  private final List<SchemaPath> columns;

  @JsonCreator
  public HttpSubScan(@JacksonInject StoragePluginRegistry registry,
                     @JsonProperty("storage") StoragePluginConfig storage,
                     @JsonProperty("scanSpec") HttpSubScanSpec scanSpec,
                     @JsonProperty("columns") List<SchemaPath> columns) throws ExecutionSetupException {
    super((String) null);
    httpStoragePlugin = (HttpStoragePlugin) registry.getPlugin(storage);
    this.scanSpec = scanSpec;
    this.storage = (HttpStoragePluginConfig) storage;
    this.columns = columns;
  }

  public HttpSubScan(HttpStoragePlugin plugin, HttpStoragePluginConfig config,
                     HttpSubScanSpec scanSpec, List<SchemaPath> columns) {
    super((String) null);
    httpStoragePlugin = plugin;
    storage = config;
    this.scanSpec = scanSpec;
    this.columns = columns;
  }

  @JsonIgnore
  public HttpStoragePluginConfig getStorageConfig() {
    return storage;
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  public HttpSubScanSpec getScanSpec() { return this.scanSpec; }

  @Override
  public boolean isExecutable() {
    return false;
  }

  @JsonIgnore
  public HttpStoragePlugin getStorageEngine(){
    return httpStoragePlugin;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new HttpSubScan(httpStoragePlugin, storage, scanSpec, columns);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  public static class HttpSubScanSpec {

    private final String tableName;

    @JsonCreator
    public HttpSubScanSpec(@JsonProperty("tableName") String tableName) {
      this.tableName = tableName;
    }

    public String getTableName() {
      return tableName;
    }

  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.HBASE_SUB_SCAN_VALUE;
  }

}
