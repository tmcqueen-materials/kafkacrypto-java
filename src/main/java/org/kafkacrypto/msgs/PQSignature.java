package org.kafkacrypto.msgs;

import org.openquantumsafe.Signature;

import org.kafkacrypto.Utils;

/*
 * The purpose of this shim is to match the final FIPS 205 SLH-DSA standard, using
 * the version 3.1 SPINCS+ implementation currently in liboqs, and to fix a
 * limitation of liboqs-java that does not set the public key when passed a secret key.
 */
public class PQSignature extends Signature
{
  private byte[] public_key_ = null;

  public PQSignature(String alg_name)
  {
    super(alg_name);
  }

  public PQSignature(String alg_name, byte[] secret_key)
  {
    super(alg_name, secret_key);
    if (alg_name.equals("SPHINCS+-SHAKE-128f-simple"))
      this.public_key_ = Utils.splitArray(secret_key, 32)[1]; // second half of secret key is public key
  }

  public byte[] export_public_key() {
    // We have to override this, since the current liboqs-java implementation does not set the public key when passed a private key.
    if (this.public_key_ != null)
      return this.public_key_;
    return super.export_public_key();
  }

  public byte[] sign(byte[] message) throws RuntimeException
  {
    // We need to add the domain separator and empty context string to match the final standard from version 3.1.
    byte[] dsctx = {0,0};
    return super.sign(Utils.concatArrays(dsctx, message));
  }

  public boolean verify(byte[] message, byte[] signature, byte[] public_key) throws RuntimeException
  {
    // We need to add the domain separator and empty context string to match the final standard from version 3.1.
    byte[] dsctx = {0,0};
    return super.verify(Utils.concatArrays(dsctx, message), signature, public_key);
  }
}
