package com.ligadata.KamanjaBase.api.java.function;

import scala.Tuple2;

public interface PairFunction<T, K, V> {
  public Tuple2<K, V> call(T t) throws Exception;
}

