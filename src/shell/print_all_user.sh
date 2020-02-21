#!/usr/bin/env bash



function tips_info() {
  echo "******************************************"
	echo "***  (1) 打印string长度"
	echo "***  (2) 在整个字符串中删除Hadoop"
	echo "***  (3) 替换第一个Hadoop为Mapreduce"
	echo "***  (4) 替换全部Hadoop为Mapreduce"
	echo "******************************************"
}
function print_len() {
    if [ -z "${*}" ];then
      echo "Error, stirng is null"
      exit 1
    else
      echo "${#*}"
    fi
}
function del_hadoop() {
    if [ -z "${*}" ];then
      echo "Error,string is null"
      exit 1
    else
      # 替换变量内的旧字符为新字符，全部替换,替换为空字符
      echo "${*//Hadoop/}"
    fi
}
function rep_hadoop_mapreduce_first() {
    string=${*}
    if [ -z "${*}" ];then
      echo "Error,stirng is null"
    else
      #替换变量内的旧字符为新字符，只替换第一个
      echo "${string/Hadoop/Mapreduce}"
      echo "${*/Hadoop/Mapreduce}"
    fi
}
function rep_hadoop_mapreduce_all() {
    if [ -z "${*}" ];then
      echo "Error,string is null"
    else
      echo "${*//Hadoop/Mapreduce}"
    fi
}
string="Bigdata process framework is Hadoop,Hadoop, Hadoop Hadoop is an open source project"
tips_info
while [ true ]; do
  echo "[string=${string}]"
  read -p "please swtich a choice: " choice
  case "${choice}" in
    1) echo
       echo "Length of string is: `print_len $string`"
       echo
       continue
    ;;
    2)
		  echo
		  echo "删除Hadoop后的字符串为：`del_hadoop $string`"
		  echo
		;;
	  3)
		  echo
		  echo "替换第一个Hadoop的字符串为：`rep_hadoop_mapreduce_first $string`"
		  echo
		;;
	  4)
		  echo
      echo "替换全部Hadoop字符串为：`rep_hadoop_mapreduce_all $string`"
      echo
		;;
	  q|Q)
		  exit 0
		;;
	  *)
		  echo "error,unlegal input,legal input only in { 1|2|3|4|q|Q }"
		  continue
		;;
  esac
done
