package yohei224.drill.exec.store.http;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class HttpScanSpec {

  private final String tableName;

  @JsonCreator
  public HttpScanSpec(@JsonProperty("tableName") String tableName) {
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }


}
