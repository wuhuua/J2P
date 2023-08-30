package com.unloadhome.extendfilter;

import org.apache.commons.lang3.StringUtils;

import java.util.UUID;

public class ExtendUUIDOperation {
    public static int uuid2Byte(String uuidString, byte[] result, int offset) {
        if (StringUtils.isNumeric(uuidString)) {
            long2bytes(Long.parseLong(uuidString), result, offset);
            return 8;
        } else {
            UUID uuid = UUID.fromString(uuidString);
            long least = uuid.getLeastSignificantBits();
            long most = uuid.getMostSignificantBits();
            long2bytes(most, result, offset);
            long2bytes(least, result, 8 + offset);
            return 16;
        }
    }

    public static void long2bytes(long value, byte[] bytes, int offset) {
        for(int i = 7; i > -1; --i) {
            bytes[offset++] = (byte)((int)(value >> 8 * i & 255L));
        }
    }


    public static String byte2UUID(byte[] data, int offset) {
        if (data.length - offset < 16) {
            return String.valueOf(byte2Long(data, offset));
        } else {
            long msb = 0L;
            long lsb = 0L;

            int i;
            for(i = offset; i < 8 + offset; ++i) {
                msb = msb << 8 | (long)(data[i] & 255);
            }

            for(i = 8 + offset; i < 16 + offset; ++i) {
                lsb = lsb << 8 | (long)(data[i] & 255);
            }

            return (new UUID(msb, lsb)).toString();
        }
    }

    public static long byte2Long(byte[] data, int offset) {
        long ret = 0L;

        for(int i = offset; i < offset + 8; ++i) {
            ret = ret << 8 | (long)(data[i] & 255);
        }

        return ret;
    }
}
