package spark.example

import play.api.libs.json._

/**
 * Created by juanpi on 2015/7/21.
 */
object jsonText {
  def main(args: Array[String]) {
    val a= "{\"extend_params\":\"jiu\",\"source\":\"\",\"starttime\":\"1437235161448\",\"deviceid\":\"CD0B8471-FD07-497C-A72A-8CC984BB6C96\",\"app_name\":\"zhe\",\"utm\":\"101431\",\"to_switch\":\"0\",\"pagename\":\"page_tab\",\"wap_u\nrl\":\"\",\"gj_page_names\":\"page_tab,page_tab,page_tab,page_tab\",\"os\":\"iOS\",\"endtime\":\"1437235209940\",\"ticks\":\"1435506281053\",\"os_version\":\"7\",\"pre_page\":\"page_goods\",\"uid\":\"0\",\"gj_ext_params\":\n\"jiu,jiu,jiu,jiu\",\"app_version\":\"3.2.4\",\"jpid\":\"76cac4ca55094fdf0d6fbb326b2c9f38d65d4ee5\",\"session_id\":\"1435506281053_zhe_1437234997980\",\"wap_pre_url\":\"http://a.m.tmall.com/i520554647759.ht\nm\",\"pre_extend_params\":\"74471824\",\"starttime_origin\":\"1437235161159\",\"endtime_origin\":\"1437235209651\",\"ip\":\"116.20.56.206\"}"
    val json = Json.parse(a)
    print(json\"utm")
  }
}
