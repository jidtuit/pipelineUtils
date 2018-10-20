package org.jid.pipelineutils.streams;

import org.paumard.streams.StreamsUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static pl.touk.throwing.ThrowingFunction.unchecked;

public class StreamBuilderUtils {

    private StreamBuilderUtils() {
    }

    /**
     *
     * Read the files line by line
     *
     * <pre>{@code
     *      Path file0 = Files.of("a00", "a01", "a02", "a03");
     *      Path file1 = Files.of("a10", "a11", "a12", "a13");
     *      Path file2 = Files.of("a20", "a21", "a22", "a23");
     *      Path file3 = Files.of("a30", "a31", "a32", "a33");
     *      Stream<String> traversingStream = StreamBuilderUtils.newStreamTraverseFiles(",", file0, file1, file2, file3);
     *      List<String> collect = traversingStream.map(st -> st.collect(Collectors.toList());
     *      // The collect list is [["a00", "a10", "a20", "a30"],
     *                            ["a01", "a11", "a21", "a31"],
     *                            ["a02", "a12", "a22", "a32"],
     *                            ["a03", "a13", "a23", "a33"]]
     * }</pre>
     *
     * WARNINGS:
     *  - Stream must be closed explicitly or inside a try-with-resources.
     *  - It doesn't support paralellism since there are files read.
     *  - If files.lenght == 1 then <code>IllegalArgumentException</code>. It must be at least 2 files.
     *  - If one file has less rows it won't fail and it would create an empty string for that section
     *
     * @param separator: The lines of each file will be joined into a String using this separator.
     * @param files: The files to iterate thorugh.
     * @return Stream of Strings linked between them with the separator.
     */
    public static Stream<String> newStreamTraverseFiles(String separator, Path... files) {
        Objects.requireNonNull(separator);
        if(files == null || files.length == 0)
            throw new PipelineUtilsException("ERROR: There must be at least one file as a parameter");

        Stream<String>[] fileStreams = Stream.of(files)
                .map(unchecked(Files::lines))
                .toArray(Stream[]::new);

        return StreamsUtils.traverse(fileStreams)
                .map(stream -> stream.collect(Collectors.joining(separator)))
                .onClose(() -> Arrays.stream(fileStreams).forEach(Stream::close));
    }


    /**
     *  Checks the size of the file and if it is less than the maxFileSize param then it will load it in memory
     *  so it can be accessed faster and process in parallel if necessary.
     *
     * @param path: File Path
     * @param maxFileSize: Maximum file size to store it in memory. More than that will be read it from file
     * @param parallelIfPossible: If the file is loadaed in memory then return a parallel stream.
     * @return an open stream
     */
    public static Stream<String> loadFileToMemoryIfSize(Path path, Long maxFileSize, Boolean parallelIfPossible) {

        Objects.requireNonNull(path);
        long lMaxFileSize = Objects.requireNonNullElse(maxFileSize, 0L);
        boolean bParallelIfPossible = Objects.requireNonNullElse(parallelIfPossible, false);

        try {
            long currentFileSize = Files.size(path);

            Stream<String> resp;

            if(currentFileSize < lMaxFileSize) {

                List<String> lines = Files.readAllLines(path);
                resp = bParallelIfPossible ? lines.parallelStream() : lines.stream();

            } else {
                resp = Files.lines(path);
            }

            return resp;

        } catch (IOException e) {
            throw new PipelineUtilsException(e);
        }
    }

}
