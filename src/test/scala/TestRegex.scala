import java.util.regex.Pattern

object TestRegex extends App {
  //  val url = "https://h5.vova.com/en/flash-sale?goods_id=7780012&virtual_goods_id=9025846&uuid=9EB05760-9B4D-4054-8891-3413F4DC0660&uid=44643463&access_token=N2FjZTcwMjBjNWY1MDczMjgxZWVlZjlkNDVlOGQ0NzJfMGUyM2JiNzcwYzIxMDA1NDFmYzNmMDAyMTVmNGE5OWI%3D&country_code=GB&currency=GBP&s=1&idfa=E5E557E5-FB9C-4CEC-8C32-0BFDFCB97110&organic_idfv=9EB05760-9B4D-4054-8891-3413F4DC0660&timezone=Europe%2FLondon&user_type=2&lang=en&statusBarHeight=88&navigationBarHeight=88&is_open_notification=1&dir=ltr"
  //
  //  val pattern = Pattern.compile("goods_id=(\\d+)")
  //  val matcher = pattern.matcher(url)
  //  if(matcher.find()){
  //    val goods_id = matcher.group(1)
  //    println(goods_id)
  //  }

  val testInfo = "gender_selected_page_0&products_sales_volume_1&new_product_detail4_&new_product_detail3_&new_detail_180926_0&Isquitcoupon_&flashsale_list_&me_myprofile_icon_&new_ProductDetail5.0_a"


  val x = testInfo.lastIndexOf("new_ProductDetail5.0")
  val testVersion = testInfo.substring(x + "new_ProductDetail5.0".length + 1)
  println(testVersion)


}
