package org.kafkacrypto;

import java.nio.file.Paths;
import java.nio.file.Files;
import java.io.RandomAccessFile;
import java.io.IOException;
import static java.nio.file.StandardCopyOption.*;

class AtomicFile
{
  private String __file, __tmpfile;
  private RandomAccessFile __readfile, __writefile;

  public AtomicFile(String file) throws IOException
  {
    this(file, file+".tmp");
  }

  public AtomicFile(String file, String tmpfile) throws IOException
  {
    this.__file = file;
    this.__tmpfile = tmpfile;
    this.__readfile = new RandomAccessFile(this.__file, "rw");
    this.__writefile = null;
  }

  private void __prepare_writefile() throws IOException
  {
   if (this.__writefile==null) {
      Files.copy(Paths.get(this.__file), Paths.get(this.__tmpfile), REPLACE_EXISTING);
      this.__writefile = new RandomAccessFile(this.__tmpfile, "rws");
      this.__writefile.seek(this.__readfile.getFilePointer());
    }
  }

  public String readLine() throws IOException
  {
    return this.__readfile.readLine();
  }

  public void seek(long pos) throws IOException
  {
    if (this.__writefile!=null)
      this.__writefile.seek(pos);
    this.__readfile.seek(pos);
  }

  public void write(byte[] b) throws IOException
  {
    this.__prepare_writefile();
    this.__writefile.write(b);
  }

  public void write(String s) throws IOException
  {
    this.__prepare_writefile();
    this.__writefile.write(s.getBytes());
  }

  public void writeLine(String line) throws IOException
  {
    this.__prepare_writefile();
    this.__writefile.write((line+"\n").getBytes());
  }

  public void truncate() throws IOException
  {
    this.__prepare_writefile();
    this.__writefile.setLength(this.__writefile.getFilePointer());
  }

  public void flush() throws IOException
  {
    if (this.__writefile != null) {
      this.__writefile.getFD().sync();
      long pos = this.__writefile.getFilePointer();
      this.__readfile.close();
      this.__writefile.close();
      Files.move(Paths.get(this.__tmpfile),Paths.get(this.__file),REPLACE_EXISTING,ATOMIC_MOVE);
      this.__readfile = new RandomAccessFile(this.__file, "rw");
      this.__readfile.seek(pos);
      this.__writefile = null;
    }
    Files.deleteIfExists(Paths.get(this.__tmpfile));
  }

  public void close() throws IOException
  {
    this.flush();
    this.__readfile.close();
  }
}
