程序入口类：mr.mrs.MrsMain
参数说明：4个参数均为必传参数 
	  参数1:输入路径 
	  参数2:输出路径 
	  参数3:厂家  (暂时只根据诺西华为两个厂家做了不同处理，其他厂家暂未处理)
	  参数4:是否自动解压 true or false
	  
	  
程序会解析路径下所有包含MRS的文件，对于相同小区不同时段的数据自动进行合并处理。
数据文件中包含的reportTime、startTime、endTime三个时间，分别取得所有文件里时间的最大值、最小值、最大值

解析结果以csv文件格式输出，按照测量类型进行区分文件