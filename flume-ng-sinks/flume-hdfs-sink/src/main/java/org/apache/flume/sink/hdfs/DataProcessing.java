/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flume.sink.hdfs;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
//import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import nl.bitwalker.useragentutils.UserAgent;

/**
 * 定制化数据粗处理
 * 
 * @author zhanghyong
 * 
 */
public class DataProcessing {

	/**
	 * The constant logger.
	 */
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(DataProcessing.class);
	// private static final String aesKey = "bbgmiduk20141010";

	/**
	 * 数据粗处理
	 * 
	 * @param data
	 * @return
	 * @throws UnsupportedEncodingException
	 * @throws ParseException
	 */
	public static HDFSdto processin(String data) throws UnsupportedEncodingException, ParseException {
		HDFSdto hdfSdto = new HDFSdto();

//		LOGGER.info("原始数据："+data);
		HashMap<String, Object> returnData = new HashMap<String, Object>();
		// data = URLDecoder.decode(data, "utf-8");;
		data = data.replace("%7C%7C", "||");
		data = data.replace("\\x22", "\"");
		data = data.replace("\"\"", "{}");
		String[] log = data.split(" - ");
		if (log.length > 1 && !log[2].isEmpty()) {
			if (!log[2].isEmpty() && log[2].indexOf("?") > 1) {
				String[] request = log[2].split("[?]");
				// 只处理a.gif 和e.gif
				String a = "/a.gif";
				String e = "/e.gif";

				HashMap<String, Object> map = new HashMap<String, Object>();
				// LOGGER.info(request[0]);
				if (request[0].equals(a)) {

					// 设置基础数据
					mapBase(map, log);

					// 处理接口数据
					HashMap<String, Object> get = StringToMap(request[1], "&", "=");
					for (Map.Entry<String, Object> v : get.entrySet()) {
						// EX数据单独处理
						if (v.getKey().equals("EX")) {
							String exStr = v.getValue().toString();
							if (!(exStr.isEmpty() || exStr.equals("%22%22") || exStr.equals("\\x22\\x22"))) {
//								LOGGER.info("decode前："+exStr);
								exStr = URLDecoder.decode(exStr, "utf-8");
//								LOGGER.info("decode后："+exStr);
								try {
									JSONObject ex = ExMap(JSONObject.parseObject(exStr));
									// String page_type = "";
									if (!ex.isEmpty()) {
										// 货品ID处理
										if (ex.containsKey("productId") && !ex.get("productId").toString().isEmpty()) {
											map.put("productId", ex.get("productId").toString());
										} else {
											map.put("productId", "");
										}
									}
									map.put("ex", ex);
								} catch (Exception exero) {
									LOGGER.info("ex解析格式错误：" + exero.getMessage() + " EX：" + data);
									map.put("ex", JSONObject.parseObject("{}"));
								}
							}
						} else {
							HashMap<String, Object> getmap = StringToMap(v.getValue().toString(), "\\|\\|", ":");
							if (!getmap.isEmpty()) {
								map.putAll(getmap);
							}
						}
					}
					// 处理 plat
					if (map.containsKey("plat")) {
						String plat = map.get("plat").toString();
						if (plat.isEmpty()) {
							map.put("plat", "h5");
						}
					} else {
						map.put("plat", "h5");
					}
					returnData.put(request[0], MapDataTOSting(map, "^"));
					hdfSdto.setCt(map.get("ct").toString());
				} else if (request[0].equals(e)) {
					// 设置基础数据
					mapBase(map, log);
					HashMap<String, Object> get = StringToMap(request[1], "&", "=");
					for (Map.Entry<String, Object> v : get.entrySet()) {
						HashMap<String, Object> getmap = StringToMap(v.getValue().toString(), "\\|\\|", ":");
						if (!getmap.isEmpty()) {
							map.putAll(getmap);
						}
					}
//					String un = map.get("un").toString();
//					String uid = "0";
//					if(!un.equals("guest")){
//						try {
//							uid = Aes.decode(aesKey.getBytes(),un.toString()).toString();
//						} catch (Exception e1) {
//							
//						}
//					}
//					map.put("un", uid);
					if (map.containsKey("plat")) {
						String plat = map.get("plat").toString();
						if (plat.isEmpty()) {
							map.put("plat", "h5");
						}
					} else {
						map.put("plat", "h5");
					}
					returnData.put(request[0], MapDataTOStingForE(map, "^"));
					hdfSdto.setCt(map.get("ct").toString());
				}
			}
		}
		hdfSdto.setMap(returnData);
		return hdfSdto;
	}

	/**
	 * 字符串转Map类型
	 * 
	 * @param data
	 * @param reg1
	 * @param reg2
	 * @return HashMap
	 * @throws UnsupportedEncodingException
	 */
	public static HashMap<String, Object> StringToMap(String data, String reg1, String reg2)
			throws UnsupportedEncodingException {
		HashMap<String, Object> map = new HashMap<String, Object>();
		String[] gets = data.split(reg1);
		for (int i = 0; i < gets.length; i++) {
			if (!gets[i].isEmpty()) {
				String[] get = gets[i].split(reg2, 2);
				if (get.length > 1) {
					if ((get[0].equals("url") || get[0].equals("ref")) && !get[1].isEmpty()) {
						get[1] = URLDecoder.decode(get[1], "utf-8");
					}
					map.put(get[0], get[1]);
				}
			}
		}
		return map;
	}

	/**
	 * 处理EX 类型的数据提取关键词
	 * 
	 * @param ex
	 * @return
	 */
	public static JSONObject ExMap(JSONObject ex) {
		JSONObject ExMaps = new JSONObject();
		if (null != ex && !ex.isEmpty()) {
			JSONArray exArr = (JSONArray) ex.get("gatherMore");
			if (null != exArr && !exArr.isEmpty()) {
				ExMaps = (JSONObject) exArr.get(0);
			} else {
				return ex;
			}
		}
		return ExMaps;
	}

	/**
	 * 获取map数据组合成字符串
	 * 
	 * @param map
	 * @param intervalStr
	 * @return
	 */
	public static String MapDataTOSting(HashMap<String, Object> map, String intervalStr) {
		String strDate = "";
		String[] wg_keys = { "plat", "ct", "cth", "ctm", "productId", "sid", "vid", "pid", "un", "ip", "dpr", "fv",
				"lv", "ws", "url", "ref", "pf", "tz", "lg", "an", "ac", "ua", "ss", "ex", "os", "br", "brv", "nld",
				"phm", "bid", "cid", "tk" };
		for (int i = 0; i < wg_keys.length; i++) {
			if (!StringUtils.isEmpty(strDate)) {
				strDate += intervalStr;
			}
			Object val = map.get(wg_keys[i]);
			if (val != null) {
				strDate += val.toString();
			}
		}
		// for (Map.Entry<String, Object> v : map.entrySet()) {
		// if (!StringUtils.isEmpty(strDate)) {
		// strDate += intervalStr;
		// }
		// strDate += v.getValue().toString();
		//
		// }
		return strDate;
	}

	/**
	 * 获取map数据组合成字符串
	 * 
	 * @param map
	 * @param intervalStr
	 * @return
	 */
	public static String MapDataTOStingForE(HashMap<String, Object> map, String intervalStr) {
		String strDate = "";
		String[] wg_keys = { "plat", "ct", "cth", "ctm", "sid", "vid", "un", "ip", "xy", "type", "code", "url", "ref",
				"href", "args", "ua", "os", "br", "brv", "dpr", "ss", "ws", "bid", "cid", "tk" };
		for (int i = 0; i < wg_keys.length; i++) {
			if (!StringUtils.isEmpty(strDate)) {
				strDate += intervalStr;
			}
			Object val = map.get(wg_keys[i]);
			if (val != null) {
				strDate += val.toString();
			}
		}

		return strDate;
	}

	public static HashMap<String, Object> mapBase(HashMap<String, Object> map, String[] log) throws ParseException {

//		map.put("id", UUID.randomUUID().toString());
		map.put("ip", log[0]);
		map.put("ua", log[1]);
		// map.put("ref", log[4]);
		// 时间处理
		String[] times = log[3].split("[+]");
		Long ct = 0L;
		String cth = "";
		String ctm = "";
		SimpleDateFormat timesCt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat timesCth = new SimpleDateFormat("HH");
		SimpleDateFormat timesCtm = new SimpleDateFormat("mm");
		if (times.length > 1 && !times[0].isEmpty()) {
			Date dataTime = timesCt.parse(times[0].replaceAll("T", " "));
			ct = dataTime.getTime();
			cth = timesCth.format(dataTime);
			ctm = timesCtm.format(dataTime);
		}
		map.put("ct", ct);
		map.put("cth", cth);
		map.put("ctm", ctm);

		map.put("os", "");
		map.put("br", "");
		map.put("brv", "");
		if (!StringUtils.isEmpty(map.get("ua").toString())) {
			String userAgent = map.get("ua").toString();
			UserAgent ua = new UserAgent(userAgent.toString());
			map.put("os", ua.getOperatingSystem().toString());
			map.put("br", ua.getBrowser().toString());
//			if(!ua.getBrowserVersion().equals(null)){
//				map.put("brv", ua.getBrowserVersion().toString());
//			}

		}

		return map;
	}

}
