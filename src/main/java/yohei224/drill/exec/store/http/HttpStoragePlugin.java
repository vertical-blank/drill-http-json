package yohei224.drill.exec.store.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;

import java.io.IOException;

public class HttpStoragePlugin extends AbstractStoragePlugin {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HttpStoragePlugin.class);

  private final DrillbitContext context;
  private final HttpStoragePluginConfig engineConfig;

  @SuppressWarnings("unused")
  private final String name;

  public HttpStoragePlugin(HttpStoragePluginConfig configuration, DrillbitContext context, String name)
      throws IOException {
    this.context = context;
    this.engineConfig = configuration;
    this.name = name;
  }

  @Override
  public void start() throws IOException {

  }

  @Override
  public void close() throws Exception {

  }

  public DrillbitContext getContext() {
    return this.context;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public HttpGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
    HttpScanSpec scanSpec = selection.getListWith(new ObjectMapper(), new TypeReference<HttpScanSpec>() {});
    return new HttpGroupScan(this, scanSpec, null);
  }

  @Override
  public boolean supportsWrite() {
    return true;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    HttpCatalogSchema schema = new HttpCatalogSchema(name);
    SchemaPlus hPlus = parent.add(name, schema);
    schema.setHolder(hPlus);
  }
  private class HttpCatalogSchema extends AbstractSchema {

    HttpCatalogSchema(String name) {
      super(ImmutableList.<String> of(), name);
    }

    @Override
    public String getTypeName() {
      return HttpStoragePluginConfig.NAME;
    }

    @Override
    public Table getTable(String url) {
      HttpScanSpec spec = new HttpScanSpec(url);
      return new DynamicDrillTable(HttpStoragePlugin.this, "http", null, spec);
    }

    public void setHolder(SchemaPlus plusOfThis) {
    }
  }

  @Override
  public HttpStoragePluginConfig getConfig() {
    return engineConfig;
  }

}