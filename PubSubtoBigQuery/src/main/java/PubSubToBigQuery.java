import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;


/**
 * PubSubToBigQuery 파이프라인은 기본적으로,
 * 1. PubSub으로부터 json 형식의 데이터를 받아서, UDF(User Defined Func)을 실행하고, 결과를 BigQuery로 전달한다.
 * 2. 변형 과정에서 일어나는 모든 에러는 에러 테이블에 기록된다.
 * 즉, 결과 및 에러 테이블이 정의되어야 한다.
 *
 * 요구 조건
 * 1. PubSub Topic - projects/netmarble-2020-intern/topics/singular-logs
 * 2.
 * 아직 정확한 빌드 및 실행 방식은 모르지만, 코드 먼저 이해하도록 하자.
 */

public class PubSubToBigQuery {
//    /** Pubsub message/string coder for pipeline. */
//    /** FailsafeElementCoder<OriginalT, CurrentT>와 같이 정의되어 있는데,
//     * 각 제네릭 변수는 payload에 해당하는데, payload란 옮겨지는 데이터인데,
//     * 실제 PubSub message 내에는 data 외에 다른 attribute가 있기 때문에,
//     * String에는 실질적 데이터만 들어있는 것이다.*/
//    public static final FailsafeElementCoder<PubsubMessage, String> CODER =
//            FailsafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of());

    /** 프로젝트 실행을 위해 필요한 option들을 나열한다. */
    public interface Options extends PipelineOptions {
        @Description("Table spec to write the output to")
        ValueProvider<String> getOutputTableSpec();

        void setOutputTableSpec(ValueProvider<String> value);

        @Description("Pub/Sub topic to read the input from")
        ValueProvider<String> getInputTopic();

        void setInputTopic(ValueProvider<String> value);

        @Description(
                "The Cloud Pub/Sub subscription to consume from. "
                        + "The name should be in the format of "
                        + "projects/<project-id>/subscriptions/<subscription-name>.")
        ValueProvider<String> getInputSubscription();

        void setInputSubscription(ValueProvider<String> value);

        @Description(
                "This determines whether the template reads from " + "a pub/sub subscription or a topic")
        @Default.Boolean(false)
        Boolean getUseSubscription();

        void setUseSubscription(Boolean value);

        @Description(
                "The dead-letter table to output to within BigQuery in <project-id>:<dataset>.<table> "
                        + "format. If it doesn't exist, it will be created during pipeline execution.")
        ValueProvider<String> getOutputDeadletterTable();

        void setOutputDeadletterTable(ValueProvider<String> value);
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        Pipeline pipeline = Pipeline.create(options);

//        /** 각 PCollection은 coder를 요구한다. Beam SDK가 자동으로 추론하긴 하지만 직접 정의해야 할 때도 있다.
//         *  Coders to describe how the elements of a given PCollection may be encoded and decoded.
//         *  마찬가지로 각 파이프라인은 CoderRegistry를 가지고 이는, 각 PCollection의 coder에 대한 정보가 있다. */
//        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
//        /**  FailsafeElementCoder<PubsubMessage, String> */
//        coderRegistry.registerCoderForType(CODER.getEncodedTypeDescriptor(), CODER);

        PCollection<PubsubMessage> messages = null;
        if (options.getUseSubscription()) {
            messages = pipeline.apply("ReadPubSubSubscription",
                    PubsubIO.readMessagesWithAttributes()
                            .fromSubscription(options.getInputSubscription()));
        } else {
            messages = pipeline.apply(
                    "ReadPubSubTopic",
                    PubsubIO.readMessagesWithAttributes().fromTopic(options.getInputTopic()));
        }

        System.out.println(messages);

    }
}
