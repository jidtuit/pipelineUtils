package org.jid.pipelineutils.streams;

public class PipelineUtilsException extends RuntimeException{

    public PipelineUtilsException(String message) {
        super(message);
    }

    public PipelineUtilsException(String message, Throwable cause) {
        super(message, cause);
    }

    public PipelineUtilsException(Throwable cause) {
        super(cause);
    }
}
