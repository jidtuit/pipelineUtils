package org.jid.pipelineutils.tests;

import org.jid.pipelineutils.streams.StreamBuilderUtils;
import org.junit.jupiter.api.Test;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

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

        //TODO: Test if the file is locked or unlocked -> Apache Commons io FileUtils.touch()??
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

}
