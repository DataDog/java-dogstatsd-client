package com.timgroup.statsd;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Map;

/**
 * MethodHandle based bridge for using JDK8+ functionality at JDK7 language level.
 * Can be removed when support for JDK7 is dropped.
 */
public class MapUtils {

    private static final MethodHandle MAP_PUT_IF_ABSENT = buildMapPutIfAbsent();
    
    /**
     * Emulates {@code Map.putIfAbsent} semantics. Replace when baselining at JDK8+.
     * @return the previous value associated with the message, or null if the value was not seen before
     */
    static Message putIfAbsent(Map<Message, Message> map, Message message) {
        if (MAP_PUT_IF_ABSENT != null) {
            try {
                return (Message) (Object) MAP_PUT_IF_ABSENT.invokeExact(map, (Object) message, (Object) message);
            } catch (Throwable ignore) {
                return putIfAbsentFallback(map, message);
            }
        }
        return putIfAbsentFallback(map, message);
    }

    /**
     * Emulates {@code Map.putIfAbsent} semantics. Replace when baselining at JDK8+.
     * @return the previous value associated with the message, or null if the value was not seen before
     */
    private static Message putIfAbsentFallback(Map<Message, Message> map, Message message) {
        if (map.containsKey(message)) {
            return map.get(message);
        }
        map.put(message, message);
        return null;
    }

    private static MethodHandle buildMapPutIfAbsent() {
        try {
            return MethodHandles.publicLookup()
                    .findVirtual(Map.class, "putIfAbsent",
                            MethodType.methodType(Object.class, Object.class, Object.class));
        } catch (Throwable ignore) {
            return null;
        }
    }
}
