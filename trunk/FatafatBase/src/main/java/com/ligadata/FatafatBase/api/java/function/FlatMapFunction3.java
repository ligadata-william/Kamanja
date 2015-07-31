package com.ligadata.KamanjaBase.api.java.function;

public interface FlatMapFunction3<T1, T2, T3, R> {
  public Iterable<R> call(T1 t1, T2 t2, T3 t3) throws Exception;
}
