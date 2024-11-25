package org.kafkacrypto.msgs;

import org.openquantumsafe.Signature;

import org.kafkacrypto.Utils;

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
    if (this.public_key_ != null)
      return this.public_key_;
    return super.export_public_key();
  }
}
