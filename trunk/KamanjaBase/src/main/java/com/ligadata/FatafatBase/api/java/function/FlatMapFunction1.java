package com.ligadata.KamanjaBase.api.java.function;

public interface FlatMapFunction1<T, R> {
  public Iterable<R> call(T t) throws Exception;
}
