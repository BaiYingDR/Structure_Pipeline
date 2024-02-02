package com.paypal.csdmp.sp.utils

import com.paypal.csdmp.sp.common.log.Logging
import com.paypal.csdmp.sp.exception.HttpBadRequestException
import okhttp3.{FormBody, Headers, MediaType, OkHttpClient, Request, RequestBody, Response, ResponseBody}

import java.net.{InetSocketAddress, Proxy}
import java.security.cert.X509Certificate
import java.util.concurrent.TimeUnit
import javax.net.ssl.{HostnameVerifier, SSLContext, SSLSession, SSLSocketFactory, TrustManager, X509TrustManager}

object HttpUtils extends Logging {

  private var paypalProxy: Proxy = null
  lazy val defaultUnsafeOkHttpClient = getUnsafeOkHttpClient()
  lazy val defaultSafeOkHttpClient = getSafeOkHttpClient()
  lazy val defaultUnsafeOkHttpClientWithPaypalProxy = getUnsafeOkHttpClientWithPaypalProxy()
  lazy val defaultSafeOkHttpClientWithPaypalProxy = getSafeOkHttpClientWithPaypalProxy()


  def setPaypalProxy(address: String, port: String): Unit = {
    paypalProxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(address, port.toInt))
  }

  def getProxySocketAddress = paypalProxy.address()

  def getUnsafeOkHttpClient(connectTime: Int = 30, readTimeout: Int = 30, writeTimeout: Int = 30, proxy: Proxy = null): OkHttpClient = {
    val manager: TrustManager =
      new X509TrustManager() {
        override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = {}

        override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = {}

        override def getAcceptedIssuers: Array[X509Certificate] = Seq.empty[X509Certificate].toArray
      }
    val trustAllCertificates: Array[TrustManager] = Seq(manager).toArray
    val sslContext: SSLContext = SSLContext.getInstance("SSL")
    sslContext.init(null, trustAllCertificates, new java.security.SecureRandom())
    val sslSocketFactory: SSLSocketFactory = sslContext.getSocketFactory
    val clientBuilder = new OkHttpClient.Builder()
      .sslSocketFactory(sslSocketFactory, trustAllCertificates(0).asInstanceOf[X509TrustManager])
      .hostnameVerifier(new HostnameVerifier {
        override def verify(hostname: String, session: SSLSession): Boolean = true
      })
      .connectTimeout(connectTime, TimeUnit.SECONDS)
      .readTimeout(readTimeout, TimeUnit.SECONDS)
      .writeTimeout(writeTimeout, TimeUnit.SECONDS)
    if (proxy != null) clientBuilder.proxy(proxy)
    clientBuilder.build()
  }

  def getSafeOkHttpClient(connectTime: Int = 30, readTimeout: Int = 30, writeTimeout: Int = 30, proxy: Proxy = null) = {
    val clientBuilder = new OkHttpClient.Builder()
      .connectTimeout(connectTime, TimeUnit.SECONDS)
      .readTimeout(readTimeout, TimeUnit.SECONDS)
      .writeTimeout(writeTimeout, TimeUnit.SECONDS)

    if (proxy != null) clientBuilder.proxy(proxy)
    clientBuilder.build()
  }


  def getUnsafeOkHttpClientWithPaypalProxy(connectTime: Int = 30, readTimeout: Int = 30, writeTimeout: Int = 30) = {
    require(paypalProxy != null, "paypal proxy should be configured in configuration file for UnsafeOkHttpClientWithPaypalProxy usage.")
    getUnsafeOkHttpClient(connectTime, readTimeout, writeTimeout, paypalProxy)
  }


  def getSafeOkHttpClientWithPaypalProxy(connectTime: Int = 30, readTimeout: Int = 30, writeTimeout: Int = 30) = {
    require(paypalProxy != null, "paypal proxy should be configured in configuration file for SafeOkHttpClientWithPaypalProxy usage.")
    getSafeOkHttpClient(connectTime, readTimeout, writeTimeout, paypalProxy)
  }

  implicit class RickOKHttp3(okHttpClient: OkHttpClient) {


    /**
     *
     * @param url
     * @param headers
     * @return
     */
    def doGet(url: String, headers: Map[String, String] = Map.empty): ResponseBody = {
      implicit import scala.collection.JavaConversions._
      val value: Headers = Headers.of(headers)
      doGet(url, value)
    }

    /**
     *
     * @param url
     * @param headers
     * @return
     */
    def doGet(url: String, headers: Headers): ResponseBody = {
      val requestBuilder: Request.Builder = new Request.Builder().url(url)
      requestBuilder.headers(headers)
      val response: Response = okHttpClient.newCall(requestBuilder.build()).execute()
      if (!response.isSuccessful) {
        throw new HttpBadRequestException(s"failed to get url $url with error code: ${response.code()} ${response.message()},error msg:${response.body.string()}")
      }
      response.body()
    }

    /**
     *
     * @param url
     * @param params
     * @param headers
     * @return
     */
    def doPost(url: String, params: Map[String, String] = Map.empty, headers: Map[String, String] = Map.empty): ResponseBody = {
      val paramsBuilder = new FormBody.Builder()
      params.foreach {
        case (key, value) => paramsBuilder.add(key, value)
      }
      val requestBuilder: Request.Builder = new Request.Builder().url(url).post(paramsBuilder.build())

      implicit import scala.collection.JavaConversions._
      requestBuilder.headers(Headers.of(headers))
      val response: Response = okHttpClient.newCall(requestBuilder.build()).execute()
      if (!response.isSuccessful) {
        throw new HttpBadRequestException(s"failed to post url $url with error code: ${response.code()} ${response.message()},error msg:${response.body.string()}")
      }
      response.body()
    }

    def postJson(url: String, params: String, headers: Map[String, String] = Map.empty) = {
      val requestBody: RequestBody = RequestBody.create(MediaType.parse("application/json;charset=utf-8"), params)
      val builder: Request.Builder = new Request.Builder().url(url).post(requestBody)

      implicit import scala.collection.JavaConversions._
      builder.headers(Headers.of(headers))
      val response: Response = okHttpClient.newCall(builder.build()).execute()
      if (!response.isSuccessful) {
        throw new HttpBadRequestException(s"failed to post url $url with error code: ${response.code()} ${response.message()},error msg:${response.body.string()}")
      }
      response.body()
    }
  }
}
