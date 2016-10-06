package yohei224.drill.exec.store.http;

import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;

public class HttpScanBatchCreator implements BatchCreator<HttpSubScan>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HttpScanBatchCreator.class);

  @Override
  public ScanBatch getBatch(FragmentContext context, HttpSubScan subScan, List<RecordBatch> children)
      throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());

    HttpSubScan.HttpSubScanSpec subScanSpec = subScan.getScanSpec();

    List<SchemaPath> columns;
    if ((columns = subScan.getColumns())==null) {
      columns = GroupScan.ALL_COLUMNS;
    }
    RecordReader reader = new HttpRecordReader(subScanSpec, columns, context);

    return new ScanBatch(subScan, context, Collections.singletonList(reader).iterator());
  }

}
