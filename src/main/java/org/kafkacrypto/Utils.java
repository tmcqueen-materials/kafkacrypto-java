package org.kafkacrypto;

import java.util.Arrays;
import java.util.Collections;
import java.math.BigInteger;

public class Utils
{
  public static String bytesToHex(byte[] input)
  {
    StringBuilder sb = new StringBuilder(2*input.length);
    for (byte b : input)
      sb.append(String.format("%02x",b));
    return sb.toString();
  }
  public static byte[] hexToBytes(String input)
  {
    byte[] rv = new byte[input.length()/2];
    for (int i = 0; i < input.length(); i += 2)
      rv[i/2] = (byte)(Integer.parseInt(input.substring(i,i+2),16));
    return rv;
  }

  public static byte[] reverseEndian(byte[] arg)
  {
    byte[] rv = Arrays.copyOf(arg,arg.length);
    Collections.reverse(Arrays.asList(rv));
    return rv;
  }

  public static byte[] littleIncrement(byte[] arg)
  {
    byte[] inp = Utils.reverseEndian(arg);
    byte[] minrv = Utils.reverseEndian(((new BigInteger(inp)).add(BigInteger.valueOf(1))).toByteArray());
    return Arrays.copyOf(minrv, arg.length);
  }

  public static byte[] concatArrays(byte[]... args)
  {
   int totlen = 0;
    for (byte[] arg : args)
      totlen += arg.length;
    byte[] rv = new byte[totlen];
    int pos = 0;
    for (byte[] arg : args) {
      System.arraycopy(arg,0,rv,pos,arg.length);
      pos += arg.length;
    }
    return rv;
  }
  public static byte[][] splitArray(byte[] arr, int... args)
  {
    byte[][] rv = new byte[args.length+1][];
    int pos = 0;
    int idx = 0;
    for (int arg : args) {
      rv[idx] = Arrays.copyOfRange(arr,pos,arg);
      idx++;
      pos += arg;
    }
    rv[idx] = Arrays.copyOfRange(arr,pos,arr.length);
    return rv;
  }

  public static double currentTime()
  {
    return System.currentTimeMillis()/1000.0;
  }
}

