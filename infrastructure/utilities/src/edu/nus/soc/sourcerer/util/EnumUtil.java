package edu.nus.soc.sourcerer.util;

import edu.nus.soc.sourcerer.util.EnumWithValue;


public class EnumUtil {
  public static <T> EnumWithValue<T> getEnumByValue(EnumWithValue<T>[] values, T value) {
    for (EnumWithValue<T> crt : values) {
      if (crt.getValue().equals(value))
        return crt;
    }
    
    return null;
  }
}
