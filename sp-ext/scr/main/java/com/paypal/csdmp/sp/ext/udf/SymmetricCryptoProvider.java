package com.paypal.csdmp.sp.ext.udf;

import com.paypal.csdmp.sp.utils.KeyMakerUtils;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * @Author: Matthew Wu
 * @Date: 2023/10/11 11:23
 */
public class SymmetricCryptoProvider {

    private static SymmetricCryptoProvider instance;

    private final SecretKeySpec key;

    public SymmetricCryptoProvider(String url, String ac) {
        String encodedKey = KeyMakerUtils.getKeyFromKeyMaker("km#secret:encrypted_csdmp_crypt_key", url, ac);
        byte[] rawKey = Base64.getDecoder().decode(encodedKey.getBytes());
        key = new SecretKeySpec(rawKey, "AES");
    }

    public String decrypt(String encryptContent) {
        if (encryptContent == null) {
            return null;
        }
        byte[] result;
        try {
            byte[] content = parseHexStr2Byte(encryptContent);
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.DECRYPT_MODE, key);
            result = cipher.doFinal(content);
        } catch (Exception e) {
            throw new RuntimeException("failed to decrypt data...", e);
        }
        return new String(result, StandardCharsets.UTF_8);
    }

    public String encrypt(String content) {
        if (content == null)
            return null;
        byte[] result;
        try {
            Cipher cipher = Cipher.getInstance("AES");
            byte[] byteContent = content.getBytes("utf-8");
            cipher.init(Cipher.ENCRYPT_MODE, key);
            result = cipher.doFinal(byteContent);
        } catch (Exception e) {
            throw new RuntimeException("failed to encrypt data...", e);
        }
        return parseByte2HexStr(result);
    }

    /**
     * convert the byte array into Hex String
     *
     * @param buf
     * @return Hex String
     */
    private String parseByte2HexStr(byte buf[]) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < buf.length; i++) {
            String hex = Integer.toHexString(buf[i] & 0xFF);
            if (hex.length() == 1) {
                hex = '0' + hex;
            }
            sb.append(hex.toUpperCase());
        }
        return sb.toString();
    }

    /**
     * convert the Hex String to Byte Array
     *
     * @param hexStr
     * @return byte array
     */
    private byte[] parseHexStr2Byte(String hexStr) {
        if (hexStr.length() < 1) {
            return null;
        }
        byte[] result = new byte[hexStr.length() / 2];
        for (int i = 0; i < hexStr.length() / 2; i++) {
            int num = Integer.parseInt(hexStr.substring(i * 2, i * 2 + 2), 16);
            result[i] = (byte) num;
        }
        return result;
    }

    public static SymmetricCryptoProvider getInstance(String url, String ac) {
        if (null == instance) {
            synchronized (SymmetricCryptoProvider.class) {
                if (null == instance) {
                    instance = new SymmetricCryptoProvider(url, ac);
                }
            }
        }
        return instance;
    }

}