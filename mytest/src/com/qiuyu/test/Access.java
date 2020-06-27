package com.qiuyu.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;

import sun.misc.BASE64Encoder;

public class Access {
	
	public static void main(String[] args) {
		try {
			String buf="http://code.tarena.com.cn";
			URL url = new URL(buf); 
			System.out.println("****"+buf.toString() ); 
			HttpURLConnection conn = (HttpURLConnection)url.openConnection(); 
			String password = "tarenacode:code_2017"; 
			BASE64Encoder base = new BASE64Encoder(); 
			String encodedPassword = base.encode(password.getBytes()); 
			conn.setRequestProperty("Authorization", "Basic " + encodedPassword); 
			conn.setRequestMethod("GET"); 
			BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			while(in.readLine()!=null){
				System.out.println(in.readLine());
			}
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (ProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
