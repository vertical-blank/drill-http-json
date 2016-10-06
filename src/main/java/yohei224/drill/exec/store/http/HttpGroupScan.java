package yohei224.drill.exec.store.http;

import com.fasterxml.jackson.annotation.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.schedule.*;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@JsonTypeName("http-scan")
public class HttpGroupScan extends AbstractGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HttpGroupScan.class);
  private static final long DEFAULT_TABLET_SIZE = 1000;

  private HttpStoragePluginConfig storagePluginConfig;
  private List<SchemaPath> columns;
  private HttpScanSpec httpScanSpec;
  private HttpStoragePlugin storagePlugin;
  private boolean filterPushedDown = false;
  private List<HttpWork> httpWorkList = Lists.newArrayList();
  private ListMultimap<Integer,HttpWork> assignments;
  private List<EndpointAffinity> affinities;


  @JsonCreator
  public HttpGroupScan(@JsonProperty("httpScanSpec") HttpScanSpec httpScanSpec,
                       @JsonProperty("storage") HttpStoragePluginConfig storagePluginConfig,
                       @JsonProperty("columns") List<SchemaPath> columns,
                       @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
    this((HttpStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig), httpScanSpec, columns);
  }

  public HttpGroupScan(HttpStoragePlugin storagePlugin, HttpScanSpec scanSpec,
                       List<SchemaPath> columns) {
    super((String) null);
    this.storagePlugin = storagePlugin;
    this.storagePluginConfig = storagePlugin.getConfig();
    this.httpScanSpec = scanSpec;
    this.columns = columns == null || columns.size() == 0? ALL_COLUMNS : columns;
    init();
  }

  private void init() {
    String tableName = httpScanSpec.getTableName();
    Collection<DrillbitEndpoint> endpoints = storagePlugin.getContext().getBits();
    Map<String,DrillbitEndpoint> endpointMap = Maps.newHashMap();
    for (DrillbitEndpoint endpoint : endpoints) {
      endpointMap.put(endpoint.getAddress(), endpoint);
    }
//    try {
//      List<LocatedTablet> locations = storagePlugin.getClient().openTable(tableName).getTabletsLocations(10000);
//      for (LocatedTablet tablet : locations) {
//        HttpWork work = new HttpWork();
//        for (Replica replica : tablet.getReplicas()) {
//          String host = replica.getRpcHost();
//          DrillbitEndpoint ep = endpointMap.get(host);
//          if (ep != null) {
//            work.getByteMap().add(ep, DEFAULT_TABLET_SIZE);
//          }
//        }
//        httpWorkList.add(work);
//      }
//    } catch (Exception e) {
//      throw new RuntimeException(e);
//    }
  }

  private static class HttpWork implements CompleteWork {

    private EndpointByteMapImpl byteMap = new EndpointByteMapImpl();

    @Override
    public long getTotalBytes() {
      return DEFAULT_TABLET_SIZE;
    }

    @Override
    public EndpointByteMap getByteMap() {
      return byteMap;
    }

    @Override
    public int compareTo(CompleteWork o) {
      return 0;
    }
  }

  /**
   * Private constructor, used for cloning.
   * @param that The HttpGroupScan to clone
   */
  private HttpGroupScan(HttpGroupScan that) {
    super(that);
    this.columns = that.columns;
    this.httpScanSpec = that.httpScanSpec;
    this.storagePlugin = that.storagePlugin;
    this.storagePluginConfig = that.storagePluginConfig;
    this.filterPushedDown = that.filterPushedDown;
    this.httpWorkList = that.httpWorkList;
    this.assignments = that.assignments;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    HttpGroupScan newScan = new HttpGroupScan(this);
    newScan.columns = columns;
    return newScan;
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    if (affinities == null) {
      affinities = AffinityCreator.getAffinityMap(httpWorkList);
    }
    return affinities;
  }


  @Override
  public int getMaxParallelizationWidth() {
    return httpWorkList.size();
  }


  /**
   *
   * @param incomingEndpoints
   */
  @Override
  public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) {
    assignments = AssignmentCreator.getMappings(incomingEndpoints, httpWorkList);
  }


  @Override
  public HttpSubScan getSpecificScan(int minorFragmentId) {
    //List<HttpWork> workList = assignments.get(minorFragmentId);
    return new HttpSubScan(storagePlugin, storagePluginConfig, new HttpSubScan.HttpSubScanSpec(getTableName()), this.columns);
  }

  // HttpStoragePlugin plugin, HttpStoragePluginConfig config,
  // List<HttpSubScanSpec> tabletInfoList, List<SchemaPath> columns
  @Override
  public ScanStats getScanStats() {
    long recordCount = 100000 * httpWorkList.size();
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, recordCount, 1, recordCount);
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new HttpGroupScan(this);
  }

  @JsonIgnore
  public HttpStoragePlugin getStoragePlugin() {
    return storagePlugin;
  }

  @JsonIgnore
  public String getTableName() {
    return getHttpScanSpec().getTableName();
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public String toString() {
    return "HttpGroupScan [HttpScanSpec="
        + httpScanSpec + ", columns="
        + columns + "]";
  }

  @JsonProperty("storage")
  public HttpStoragePluginConfig getStorageConfig() {
    return this.storagePluginConfig;
  }

  @JsonProperty
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty
  public HttpScanSpec getHttpScanSpec() {
    return httpScanSpec;
  }

  @Override
  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return false;
  }

  /**
   * Empty constructor, do not use, only for testing.
   */
  @VisibleForTesting
  public HttpGroupScan() {
    super((String)null);
  }

  /**
   * Do not use, only for testing.
   */
  @VisibleForTesting
  public void setHttpScanSpec(HttpScanSpec httpScanSpec) {
    this.httpScanSpec = httpScanSpec;
  }

}
