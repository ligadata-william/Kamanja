package com.ligadata.FatafatBase.api.java.function;

public interface FlatMapFunction2<T1, T2, R> {
  public Iterable<R> call(T1 t1, T2 t2) throws Exception;
}
