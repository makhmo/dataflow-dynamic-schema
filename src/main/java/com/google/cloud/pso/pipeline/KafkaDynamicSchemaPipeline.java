/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.pso.pipeline;

import com.google.cloud.pso.bigquery.BigQuerySchemaMutator;
import com.google.cloud.pso.bigquery.TableRowWithSchema;
import com.google.cloud.pso.dofn.KafkaAvroToTableRowFn;
import com.google.cloud.pso.dofn.PubsubAvroToTableRowFn;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.joda.time.Duration;

import java.util.Map;

/**
 * TODO: Document.
 */
public class KafkaDynamicSchemaPipeline {

    /**
     * TODO: Document.
     */
    public interface PipelineOptions extends BigQueryPipelineOptions, StreamingOptions {

        @Description("Kafka bootstrap server list in the format host1:port1,host2:port2...")
        @Default.String("localhost:9092")
        @Validation.Required
        String getKafkaBootstrapServer();

        void setKafkaBootstrapServer(String value);

        @Description("Kafka topic to read messages from")
        @Default.String("kafka-topic-1")
        @Validation.Required
        String getTopic();

        void setTopic(String value);

        @Description("Consumer group")
        @Default.String("kafka-bq-client")
        String getConsumerGroup();

        void setConsumerGroup(String value);

        @Description("What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server ")
        @Default.String("latest")
        String getConsumerOffsetReset();

        void setConsumerOffsetReset(String value);
    }

    /**
     * TODO: Document.
     *
     * @param args
     */
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);

        run(options);
    }


    public static class ReadKafka extends PTransform<PBegin, PCollection<KafkaRecord<byte[], byte[]>>> {

        @Override
        public PCollection<KafkaRecord<byte[], byte[]>> expand(PBegin pBegin) {
            PipelineOptions options = (PipelineOptions) pBegin.getPipeline().getOptions();

            KafkaIO.Read<byte[], byte[]> kafkaRead = KafkaIO.<byte[], byte[]>read()
                    .withBootstrapServers(options.getKafkaBootstrapServer())
                    .withTopic(options.getTopic())
                    .withKeyDeserializerAndCoder(ByteArrayDeserializer.class, ByteArrayCoder.of())
                    .withValueDeserializerAndCoder(ByteArrayDeserializer.class, ByteArrayCoder.of())
                    .updateConsumerProperties(ImmutableMap.of(ConsumerConfig.GROUP_ID_CONFIG, options.getConsumerGroup(),
                            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, options.getConsumerOffsetReset()));

            PCollection<KafkaRecord<byte[], byte[]>> kafkaRecords = pBegin.apply("Read from Kafka", kafkaRead);

            return kafkaRecords;
        }
    }

    /**
     * TODO: Document.
     *
     * <p>TODO: Refactor so the pipeline structure can be tested without the sources / sinks
     *
     * @param options
     * @return
     */
    public static PipelineResult run(PipelineOptions options) {

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        // Execute pipeline and get the write result so we can react to the failed inserts
        PCollection<TableRowWithSchema> incomingRecords =
                pipeline
                        .apply("ReadAvroMessages", new ReadKafka())
                        .apply("KafkaAvroToTableRowFn",
                                ParDo.of(new KafkaAvroToTableRowFn())
                                        .withOutputTags(
                                                KafkaAvroToTableRowFn.MAIN_OUT,
                                                TupleTagList.of(KafkaAvroToTableRowFn.DEADLETTER_OUT)))
                        .get(PubsubAvroToTableRowFn.MAIN_OUT)
                        .apply("1mWindow", Window.into(FixedWindows.of(Duration.standardMinutes(1L))));


        WriteResult writeResult =
                incomingRecords.apply(
                        "WriteToBigQuery",
                        BigQueryIO.<TableRowWithSchema>write()
                                .withFormatFunction(TableRowWithSchema::getTableRow)
                                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));

        // Create side-input to join records with their incoming schema
        PCollectionView<Map<Integer, TableRowWithSchema>> incomingRecordsView =
                incomingRecords
                        .apply("KeyIncomingByHash", WithKeys.of(record -> record.getTableRow().hashCode()))
                        .apply("CreateView", View.asMap());

        // Process the failed inserts by mutating the output table schema and retrying the insert
        writeResult
                .getFailedInserts()
                .apply("MutateSchema", BigQuerySchemaMutator.mutateWithSchema(incomingRecordsView))
                .apply(
                        "RetryWriteMutatedRows",
                        BigQueryIO.<TableRowWithSchema>write()
                                .withFormatFunction(TableRowWithSchema::getTableRow)
                                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                                .withWriteDisposition(WriteDisposition.WRITE_APPEND));

        return pipeline.run();
    }
}
