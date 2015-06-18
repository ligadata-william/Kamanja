package com.ligadata.FatafatBase.api.java.function;

public interface Function2<T1, T2, R> {
  public R call(T1 v1, T2 v2) throws Exception;
}

