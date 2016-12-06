package ru.sbrf.ofep.kafka.elastic.exceptions;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.gateway.GatewayException;
import org.elasticsearch.http.HttpException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.netty.SizeHeaderFrameDecoder;

import java.util.Set;

public class ExceptionHelper {

    //TODO: need to expanded
    private static final Set<Class<?>> CONNECTION_LOST_EXCEPTIONS = ImmutableSet.<Class<?>>builder()
            .add(FailedNodeException.class)
            .add(NoNodeAvailableException.class)
            .add(TransportException.class)
            .add(NodeClosedException.class)
            .add(HttpException.class)
            .add(GatewayException.class)
            .add(MasterNotDiscoveredException.class)
            .add(SizeHeaderFrameDecoder.HttpOnTransportException.class)
            .build();

    public static boolean isCauseConnectionLost(Throwable origCause) {
        for(Class<?> cls : CONNECTION_LOST_EXCEPTIONS) {
            if(cls.isAssignableFrom(origCause.getClass())) {
                return true;
            }
        }
        final Throwable cause = origCause.getCause();
        return cause != null && isCauseConnectionLost(cause);
    }
}
