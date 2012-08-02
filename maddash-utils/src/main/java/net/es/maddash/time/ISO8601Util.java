package net.es.maddash.time;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class ISO8601Util {

    static public String fromTimestamp(long timestamp){
        DateTime dt = new DateTime(timestamp);
        return dt.toDateTimeISO().withZone(DateTimeZone.UTC).toString();
    }
}
