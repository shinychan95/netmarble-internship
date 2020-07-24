package org.apache.beam.examples;


import afu.org.checkerframework.checker.nullness.qual.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import javax.xml.soap.Text;

// That can make your pipeline easier to read, write, adn maintain. While not explicitly required.
public class WordCount {

    // core transforms 중 ParDo processing을 위해 DoFn 객체 형태로 정의
    static class ExtractWordsFn extends DoFn<String, String> {
        // Metric 선언 및 사용에 대해서는 나중에 자세히 알아보기
        private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
        private final Distribution lineLenDist = Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

        // @ProcessElement - DoFn subclass 내에 실제 processing 로직이 있는 부분. custom annotation
        // @Element - which will be populated with the input element (@ProcessElement 함께 꼭 온다)
        // OutputReceiver - which provides a method for emitting elements(@ProcessElement 함께 꼭 온다)
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            lineLenDist.update(element.length());
            if (element.trim().isEmpty()) {
                emptyLines.inc();
            }

            // 나중에 Common 그룹에 Utils 내에 정규식 등을 정의한다.
            String[] words = element.split("[^\\p{L}]+", -1);

            for (String word : words){
                if (!word.isEmpty()) {
                    receiver.output(word);
                }
            }
        }
    }

    // 아마도 Pipeline 객체에 apply 할 때 간단한 처리는 SimpleFunction을 적용하여 처리하는 듯 하다.
    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + ": " + input.getValue();
        }
    }

    // multiple transforms나 ParDo 과정들을 처리한다면 PTransform을 활용하면 된다.
    // composite transform이라고도 부른다.
    public static class CountWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
        // 실제로 일어나는 processing logic을 정의하기 위해 expand method를 오버라이딩한다.
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {
            // 문자열에서 단어를 추출하는 과정에서 ParDo를 통해 직접적으로 input, output을 주지 않고, 편하게 처리한다.
            PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));

            PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());

            return wordCounts;
        }

    }

    public interface WordCountOptions extends PipelineOptions {

        /**
         * By default, this example reads from a public dataset containing the text of King Lear. Set
         * this option to choose a different input file or glob.
         */
        @Description("Path of the file to read from")
        @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
        String getInputFile();
        void setInputFile(String value); // = public abstract void 추상메소드명

        /** Set this required option to specify where to write the output. */
        @Description("Path of the file to write to")
        @Default.String("./word-count-practice/") // 이 부분 왜인지는 모르겠지만, "./"으로 하면 프로젝트 상위 폴더에 저장된다.
        String getOutput();                       // Default 값을 이렇게 넣는 것도 Custom Annotation
        void setOutput(String value);

        @Description("My custom command line argument.")
        @Default.String("DEFAULT")
        String getMyCustomOption();
        void setMyCustomOption(String myCustomOption);
    }

    static void runWordCount(WordCountOptions options) {
        Pipeline p = Pipeline.create(options);

        p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply(new CountWords())
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply("WriteCounts", TextIO.write().to(options.getOutput()));

        p.run().waitUntilFinish();

    }

    public static void main(String[] args) {
        // 입력 변수(Runner, input, output 등)에 대한 정의는 이게 최선인 것 같다.
        // 또 의문인 점은 예시에서는 input file 및 output에 대해 정의하는데 정의하면 output이 발생되고 파이프라인이 종료된다.
        WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCountOptions.class);

        runWordCount(options);
    }



}
