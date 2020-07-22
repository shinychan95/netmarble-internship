package org.apache.beam.examples;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import javax.xml.soap.Text;

// That can make your pipeline easier to read, write, adn maintain. While not explicitly required.
public class WordCount {

    // ParDo Transform을 위해 DoFn 객체 형태로 정의한다.
    static class ExtractWordsFn extends DoFn<String, String> {
        private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
        private final Distribution lineLenDist = Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

        // DoFn subclass 내에 항상 annotated 되야 하는 부분. 실제 프로세싱 로직이 있느는 부분이다.
        @ProcessElement            // input 및 output을 위해 @Element annotation 및 OutputReceiver를 설정해야 한다.
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            lineLenDist.update(element.length());
            if (element.trim().isEmpty()) {
                emptyLines.inc();
            }

            String[] words = element.split("[^\\p{L}]+", -1);
            
            for (String word : words){
                if (!word.isEmpty()) {
                    receiver.output(word);
                }
            }
        }
    }

    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + ": " + input.getValue();
        }
    }

    public static class CountWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {
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

        void setInputFile(String value);

        /** Set this required option to specify where to write the output. */
        @Description("Path of the file to write to")
        @Default.String("./word-count-practice/")
        String getOutput();

        void setOutput(String value);
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
        WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCountOptions.class);

        runWordCount(options);
    }



}
