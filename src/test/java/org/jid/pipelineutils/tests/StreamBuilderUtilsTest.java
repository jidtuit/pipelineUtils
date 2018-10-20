package org.jid.pipelineutils.tests;

import org.jid.pipelineutils.streams.PipelineUtilsException;
import org.jid.pipelineutils.streams.StreamBuilderUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StreamBuilderUtilsTest {

    private Path baseDataDir = FileSystems.getDefault().getPath("src", "test",
            "resources", "data", "streamBuilderUtils");

    @Test
    void newStreamTraverseFiles_OK_shouldTraverseThrough3files() {

        Path[] files = Stream.of("data1.csv", "data2.csv", "data3.csv")
                            .map(baseDataDir::resolve)
                            .toArray(Path[]::new);

        Stream<String> result = StreamBuilderUtils.newStreamTraverseFiles(";", files);
        assertThat(result).isNotNull();

        List<String> resultList = result.collect(Collectors.toList());

        result.close();

        assertThat(resultList).size().isEqualTo(4);

        assertThat(resultList.get(0))
                .isEqualTo("header1-1;header1-2;header1-3;header2-1;header2-2;header2-3;header3-1;header3-2;header3-3");

        assertThat(resultList.get(1))
                .isEqualTo("value1-1-1;value1-1-2;value1-1-3;value2-1-1;value2-1-2;value2-1-3;value3-1-1;value3-1-2;value3-1-3");

        assertThat(resultList.get(2))
                .isEqualTo("value1-1-2;value1-2-2;value1-3-2;value2-1-2;value2-2-2;value2-3-2;value3-1-2;value3-2-2;value3-3-2");

        assertThat(resultList.get(3))
                .isEqualTo("value1-1-3;value1-2-3;value1-3-3;value2-1-3;value2-2-3;value2-3-3;value3-1-3;value3-2-3;value3-3-3");

    }


    @Test
    void newStreamTraverseFiles_OK_shouldTraverseThrough2filesOneIncomplete() {
        Path[] files = Stream.of("data1.csv", "dataLessRows4.csv")
                .map(baseDataDir::resolve)
                .toArray(Path[]::new);

        Stream<String> result = StreamBuilderUtils.newStreamTraverseFiles(";", files);
        assertThat(result).isNotNull();

        List<String> resultList = result.collect(Collectors.toList());

        result.close();

        assertThat(resultList.get(0))
                .isEqualTo("header1-1;header1-2;header1-3;header4-1;header4-2;header4-3");

        assertThat(resultList.get(1))
                .isEqualTo("value1-1-1;value1-1-2;value1-1-3;value4-1-1;value4-1-2;value4-1-3");

        assertThat(resultList.get(2))
                .isEqualTo("value1-1-2;value1-2-2;value1-3-2;");

        assertThat(resultList.get(3))
                .isEqualTo("value1-1-3;value1-2-3;value1-3-3;");
    }


    @Test
    void newStreamTraverseFiles_KO_nullParams() {

        String separator = ",";

        Path[] filesWithoutNull = {baseDataDir.resolve("data1.csv"), baseDataDir.resolve("data2.csv")};
        Path[] filesWithNull = {baseDataDir.resolve("data1.csv"), null, baseDataDir.resolve("data2.csv")};
        Path[] emptyFileArray = {};
        Path[] filesWithNonExistingFile = {baseDataDir.resolve("data1.csv"), baseDataDir.resolve("notExistingFile.csv")};

        assertThatThrownBy(() -> StreamBuilderUtils.newStreamTraverseFiles(null, filesWithoutNull))
                .isInstanceOf(NullPointerException.class);


        assertThatThrownBy(() -> StreamBuilderUtils.newStreamTraverseFiles(separator))
                .isInstanceOf(PipelineUtilsException.class)
                .hasStackTraceContaining("There must be at least one file as a parameter");


        assertThatThrownBy(() -> StreamBuilderUtils.newStreamTraverseFiles(separator, null))
                .isInstanceOf(PipelineUtilsException.class)
                .hasStackTraceContaining("There must be at least one file as a parameter");


        assertThatThrownBy(() -> StreamBuilderUtils.newStreamTraverseFiles(separator, emptyFileArray))
                .isInstanceOf(PipelineUtilsException.class)
                .hasStackTraceContaining("There must be at least one file as a parameter");


        assertThatThrownBy(() -> StreamBuilderUtils.newStreamTraverseFiles(separator, filesWithNull))
                .hasRootCauseInstanceOf(NullPointerException.class);


        assertThatThrownBy(() -> StreamBuilderUtils.newStreamTraverseFiles(separator, filesWithNonExistingFile))
                .hasRootCauseInstanceOf(NoSuchFileException.class);

    }


    @Test
    void loadFileToMemoryOrFromDisk_OK_FromMemoryInParallel() throws IOException {

        Path file = baseDataDir.resolve("data1.csv");
        long fileSize = Files.size(file);

        Stream<String> stream = StreamBuilderUtils.loadFileToMemoryIfSize(file, fileSize + 1, true);

        assertThat(stream).isNotNull();
        assertThat(stream.isParallel()).isTrue();
        assertThat(stream.count()).isNotZero().isNotNegative();

        stream.close();
    }


    @Test
    void loadFileToMemoryOrFromDisk_OK_FromMemorySequencial() throws IOException {

        Path file = baseDataDir.resolve("data1.csv");
        long fileSize = Files.size(file);

        Stream<String> stream = StreamBuilderUtils.loadFileToMemoryIfSize(file, fileSize + 1, false);

        assertThat(stream).isNotNull();
        assertThat(stream.isParallel()).isFalse();
        assertThat(stream.count()).isNotZero().isNotNegative();

        stream.close();

    }


    @Test
    void loadFileToMemoryOrFromDisk_OK_FromDiskSequencial() throws IOException {

        Path file = baseDataDir.resolve("data1.csv");
        long fileSize = Files.size(file);

        Stream<String> stream = StreamBuilderUtils.loadFileToMemoryIfSize(file, fileSize - 1, false);

        assertThat(stream).isNotNull();
        assertThat(stream.isParallel()).isFalse();
        assertThat(stream.count()).isNotZero().isNotNegative();

        stream.close();
    }


    @Test
    void loadFileToMemoryOrFromDisk_OK_FromDiskParallelShouldBeSequencial() throws IOException {

        Path file = baseDataDir.resolve("data1.csv");
        long fileSize = Files.size(file);

        Stream<String> stream = StreamBuilderUtils.loadFileToMemoryIfSize(file, fileSize - 1, true);

        assertThat(stream).isNotNull();
        assertThat(stream.isParallel()).isFalse();
        assertThat(stream.count()).isNotZero().isNotNegative();

        stream.close();
    }


    @Test
    void loadFileToMemoryOrFromDisk_KO_NullParams() throws IOException {

        Path file = baseDataDir.resolve("data1.csv");
        long size = Files.size(file);

        Path nonExistingfile = baseDataDir.resolve("nonExistingFile.jid");


        assertThatThrownBy(() -> StreamBuilderUtils.loadFileToMemoryIfSize(null, size, true))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> StreamBuilderUtils.loadFileToMemoryIfSize(nonExistingfile, size, true))
                .hasRootCauseInstanceOf(NoSuchFileException.class);


        Stream<String> s1 = StreamBuilderUtils.loadFileToMemoryIfSize(file, null, true);
        assertThat(s1).isNotNull();
        assertThat(s1.isParallel()).isFalse();
        assertThat(s1.count()).isNotZero().isNotNegative();
        s1.close();

        Stream<String> s2 = StreamBuilderUtils.loadFileToMemoryIfSize(file, null, false);
        assertThat(s2).isNotNull();
        assertThat(s2.isParallel()).isFalse();
        assertThat(s2.count()).isNotZero().isNotNegative();
        s2.close();

        Stream<String> s3 = StreamBuilderUtils.loadFileToMemoryIfSize(file, size + 1, null);
        assertThat(s3).isNotNull();
        assertThat(s3.isParallel()).isFalse();
        assertThat(s3.count()).isNotZero().isNotNegative();
        s3.close();
    }

}
