package org.apache.beam.examples;

// 패키지 종속성
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.util.Arrays;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;


public class MinimalWordCount {
    public static void main(String[] args){
        // 아래 코드를 적기 위해서 상단에 패키지 선언이 필요
        PipelineOptions options = PipelineOptionsFactory.create();
        System.out.println(options);

        // 오류 발생 - No Runner was specified and the DirectRunner was not found on the classpath.
        Pipeline p = Pipeline.create(options);
        System.out.println(options);

        // DataflowRunner를 설정하는데 있어서, DataflowPipelineOptions이 객체 예시가 있는 것 같지만,
        // dependency를 찾을 수가 없고, Google Cloud 내에서도 다른 방법으로 설정하도록 설명한다.
        // 일단 여기는 WordCount라 링크만 던지기. https://cloud.google.com/dataflow/docs/guides/specifying-exec-params

        // apply가 무엇인지 객체 내부에서 살펴보다가 공식 문서를 보다가 그냥 공식 문서 내 활용 예시만 살펴보았다.
        // 오류 발생 -  No filesystem found for scheme gs --> 이 또한 pom.xml을 수정해야 한다.
        p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/*"))
                // 아래 표현식 - Lambda Expression. 처음에 오류 발생하였는데, maven-compiler-plugin 플러그인 추가해야 한다.
                // 또한 Project Structure 내에 language level 또한 다시 설정해야 한다.
                .apply("ExtractWords",
                        FlatMapElements.into(TypeDescriptors.strings()) // into(TypeDescriptor<OutputT> outputType)
                                .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))

                // 모든 줄에 대한 해석을 하고 싶지만, WordCount 예제로 자바에 대한 감을 잡고,
                // 공식 문서로 제대로 이해하자. https://beam.apache.org/documentation/programming-guide/
                .apply(Filter.by((String word) -> !word.isEmpty()))
                .apply(Count.perElement())
                .apply(
                        MapElements.into(TypeDescriptors.strings())
                                .via(
                                        (KV<String, Long> wordCount) ->
                                                wordCount.getKey() + ": " + wordCount.getValue()))
                .apply(TextIO.write().to("wordcounts"));


        // 파이프라인 내 데이터는 어떻게 잘 처리되고 있는지 확인할까
        System.out.println("찬영아 퇴근이 하고 싶니?");
        System.out.println(p.toString());
        System.out.println(p.getClass().getName());


        p.run().waitUntilFinish();
    }
}
