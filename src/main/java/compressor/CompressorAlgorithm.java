package compressor;

public enum CompressorAlgorithm {
    // delta of delta algorithm
    DELTA_2,

    VAR_INT,

    // simple8b algorithm
    SIMPLE_8B,

    // fixed-length delta
    DELTA
}
