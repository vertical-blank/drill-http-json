package yohei224.drill.exec.store.http;

import org.apache.drill.common.logical.StoragePluginConfigBase;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(HttpStoragePluginConfig.NAME)
public class HttpStoragePluginConfig extends StoragePluginConfigBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HttpStoragePluginConfig.class);

  public static final String NAME = "http";

  @JsonCreator
  public HttpStoragePluginConfig() {
    //this.url = url;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    //result = prime * result + ((url == null) ? 0 : url.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    HttpStoragePluginConfig other = (HttpStoragePluginConfig) obj;
//    if (url == null) {
//      if (other.url != null) {
//        return false;
//      }
//    } else if (!url.equals(other.url)) {
//      return false;
//    }
    return true;
  }



}
