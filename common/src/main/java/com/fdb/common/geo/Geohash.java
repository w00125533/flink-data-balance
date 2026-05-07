package com.fdb.common.geo;

public final class Geohash {

    private static final char[] BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz".toCharArray();

    private Geohash() {}

    public static String encode(double lat, double lon, int precision) {
        if (precision < 1 || precision > 12) {
            throw new IllegalArgumentException("precision must be in [1, 12], got " + precision);
        }
        if (lat < -90.0 || lat > 90.0) {
            throw new IllegalArgumentException("lat out of range: " + lat);
        }
        if (lon < -180.0 || lon > 180.0) {
            throw new IllegalArgumentException("lon out of range: " + lon);
        }

        double latLo = -90.0, latHi = 90.0;
        double lonLo = -180.0, lonHi = 180.0;

        StringBuilder sb = new StringBuilder(precision);
        boolean evenBit = true;
        int bit = 0;
        int ch = 0;

        while (sb.length() < precision) {
            if (evenBit) {
                double mid = (lonLo + lonHi) / 2.0;
                if (lon >= mid) {
                    ch = (ch << 1) | 1;
                    lonLo = mid;
                } else {
                    ch = ch << 1;
                    lonHi = mid;
                }
            } else {
                double mid = (latLo + latHi) / 2.0;
                if (lat >= mid) {
                    ch = (ch << 1) | 1;
                    latLo = mid;
                } else {
                    ch = ch << 1;
                    latHi = mid;
                }
            }
            evenBit = !evenBit;
            bit++;
            if (bit == 5) {
                sb.append(BASE32[ch]);
                bit = 0;
                ch = 0;
            }
        }
        return sb.toString();
    }
}
