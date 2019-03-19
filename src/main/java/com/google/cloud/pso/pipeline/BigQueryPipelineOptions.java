package com.google.cloud.pso.pipeline;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface BigQueryPipelineOptions extends PipelineOptions {

  @Description("The BigQuery table to write messages to")
  @Validation.Required
  ValueProvider<String> getTable();

  void setTable(ValueProvider<String> value);
}
