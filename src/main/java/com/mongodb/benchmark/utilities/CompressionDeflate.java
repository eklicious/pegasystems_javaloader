package com.mongodb.benchmark.utilities;

import com.mongodb.benchmark.utilities.core_utils.StackTrace;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.Deflater;
import static java.util.zip.Deflater.DEFAULT_COMPRESSION;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompressionDeflate {
    
    private static final Logger logger = LoggerFactory.getLogger(CompressionDeflate.class.getName());
    
    public static byte[] mysqlCompatibleCompressDataWithDeflate(byte[] data, int compressLevel) {
        
        if (((compressLevel < 0) || (compressLevel > 9)) && (compressLevel != DEFAULT_COMPRESSION)) {
            logger.error("Invalid compression level.");
            return null;
        }
        
        if ((data == null) || (data.length == 0)) {
            return null;
        }

        // compress the data
        byte[] compressedData = compressDataWithDeflate(data, compressLevel);
        
        // mysql needs a prefix of '4-byte length of the uncompressed string (low byte first)' for the built-in compress & uncompress functions to work
        long dataOriginalSize = data.length;
        long dataOriginalSizeBitShifted = dataOriginalSize;
        byte dataSizeByte1 = (byte) (dataOriginalSizeBitShifted & 0xFF);
        dataOriginalSizeBitShifted = dataOriginalSizeBitShifted >> 8;
        byte dataSizeByte2 = (byte) (dataOriginalSizeBitShifted & 0xFF);
        dataOriginalSizeBitShifted = dataOriginalSizeBitShifted >> 8;
        byte dataSizeByte3 = (byte) (dataOriginalSizeBitShifted & 0xFF);
        dataOriginalSizeBitShifted = dataOriginalSizeBitShifted >> 8;
        byte dataSizeByte4 = (byte) (dataOriginalSizeBitShifted & 0xFF);

        byte[] compressedDataWithMysqlSizePrefix = new byte[compressedData.length + 4];
        compressedDataWithMysqlSizePrefix[0] = dataSizeByte1;
        compressedDataWithMysqlSizePrefix[1] = dataSizeByte2;
        compressedDataWithMysqlSizePrefix[2] = dataSizeByte3;
        compressedDataWithMysqlSizePrefix[3] = dataSizeByte4;
        
        System.arraycopy(compressedData, 0, compressedDataWithMysqlSizePrefix, 4, compressedData.length);
        
        return compressedDataWithMysqlSizePrefix;
    }
    
    public static byte[] mysqlCompatibleDecompressDeflateData(byte[] data) {
        
        if ((data == null) || (data.length == 0)) {
            return null;
        }
        
        byte[] compressedDataNoMysqlSizePrefix = new byte[data.length - 4];

        for (int i = 0; i < (data.length - 4); i++) {
            compressedDataNoMysqlSizePrefix[i] = data[i + 4];
        }

        byte[] decompressedData = decompressDeflateData(compressedDataNoMysqlSizePrefix);
        
        return decompressedData;
    }
    
    public static byte[] compressDataWithDeflate(byte[] data, int compressLevel) {
        
        if (((compressLevel < 0) || (compressLevel > 9)) && (compressLevel != DEFAULT_COMPRESSION)) {
            logger.error("Invalid compression level.");
            return null;
        }
        
        if ((data == null) || (data.length == 0)) {
            return null;
        }
        
        byte[] compressedOutput = null;
        
        ByteArrayOutputStream byteArrayOutputStream = null;
        BufferedOutputStream bufferedOutputStream = null;
        DeflaterOutputStream deflaterOutputStream = null;
        Deflater deflater = null;
        
        try {
            byteArrayOutputStream = new ByteArrayOutputStream();
            bufferedOutputStream = new BufferedOutputStream(byteArrayOutputStream);
            
            deflater = new Deflater(compressLevel);
            
            deflaterOutputStream = new DeflaterOutputStream(bufferedOutputStream, deflater);
            deflaterOutputStream.write(data);
            deflaterOutputStream.finish();
            deflaterOutputStream.flush();
            
            compressedOutput = byteArrayOutputStream.toByteArray();
        }
        catch (Exception e) {
            logger.error(e.toString() + System.lineSeparator() + StackTrace.getStringFromStackTrace(e));
        }
        finally {
            
            if (deflater != null) {
                try {
                    deflater.end();
                }
                catch (Exception e) {
                    logger.error(e.toString() + System.lineSeparator() + StackTrace.getStringFromStackTrace(e));
                }
            }
            
            if (deflaterOutputStream != null) {
                try {
                    deflaterOutputStream.close();
                }
                catch (Exception e) {
                    logger.error(e.toString() + System.lineSeparator() + StackTrace.getStringFromStackTrace(e));
                }
            }
            
            if (bufferedOutputStream != null) {
                try {
                    bufferedOutputStream.close();
                }
                catch (Exception e) {
                    logger.error(e.toString() + System.lineSeparator() + StackTrace.getStringFromStackTrace(e));
                }
            }
            
            if (byteArrayOutputStream != null) {
                try {
                    byteArrayOutputStream.close();
                }
                catch (Exception e) {
                    logger.error(e.toString() + System.lineSeparator() + StackTrace.getStringFromStackTrace(e));
                }
            }
        }
        
        return compressedOutput;
    }
    
    public static byte[] decompressDeflateData(byte[] data) {
        
        if ((data == null) || (data.length == 0)) {
            return null;
        }
        
        byte[] decompressedOutput = null;
        
        ByteArrayOutputStream byteArrayOutputStream = null;
        InflaterOutputStream inflaterOutputStream = null;
        Inflater inflater = null;
        
        try {
            byteArrayOutputStream = new ByteArrayOutputStream();

            inflater = new Inflater();
            inflater.setInput(data);
            
            inflaterOutputStream = new InflaterOutputStream(byteArrayOutputStream, inflater);
            inflaterOutputStream.write(data);
            inflaterOutputStream.finish();
            inflaterOutputStream.flush();
            
            decompressedOutput = byteArrayOutputStream.toByteArray();
        }
        catch (Exception e) {
            logger.error(e.toString() + System.lineSeparator() + StackTrace.getStringFromStackTrace(e));
        }
        finally {
            
            if (inflater != null) {
                try {
                    inflater.end();
                }
                catch (Exception e) {
                    logger.error(e.toString() + System.lineSeparator() + StackTrace.getStringFromStackTrace(e));
                }
            }
            
            if (inflaterOutputStream != null) {
                try {
                    inflaterOutputStream.close();
                }
                catch (Exception e) {
                    logger.error(e.toString() + System.lineSeparator() + StackTrace.getStringFromStackTrace(e));
                }
            }
            
            if (byteArrayOutputStream != null) {
                try {
                    byteArrayOutputStream.close();
                }
                catch (Exception e) {
                    logger.error(e.toString() + System.lineSeparator() + StackTrace.getStringFromStackTrace(e));
                }
            }
        }
        
        return decompressedOutput;
    }    
 
}
