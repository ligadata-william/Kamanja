package com.ligadata.KamanjaBase.api.java.function;

public interface Function3<T1, T2, T3, R> {
  public R call(T1 v1, T2 v2, T3 v3) throws Exception;
}
