package com.google.cloud.pso.pipeline;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface BigQueryPipelineOptions extends PipelineOptions {

    @Description("The BigQuery table to write messages to")
    @Validation.Required
    String getTable();

    void setTable(String value);
}
