package ch.daplab.hivepartition.rx;

import com.verisign.vscc.hdfs.trumpet.dto.EventAndTxId;
import rx.functions.Func1;

import java.util.Map;

public class CreateDirectoryEventFilter implements Func1<Map<String, Object>, Boolean> {
    @Override
    public Boolean call(Map<String, Object> event) {
        String eventType = (String) event.get(EventAndTxId.FIELD_EVENTTYPE);
        String nodeType = (String) event.get("iNodeType");
        return "CREATE".equals(eventType) && "DIRECTORY".equals(nodeType);
    }
}
