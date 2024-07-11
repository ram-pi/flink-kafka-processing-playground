package com.github.rampi;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAlias;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class AggregatedEvent {
  @JsonAlias("start_time")
  Long startTime;

  @JsonAlias("end_time")
  Long endTime;

  @JsonAlias("count")
  int count;
}
