1.conf文件夹存放基本配置文件（包含cfg和pool文件）
	lib存放必要的包
	log存放日志文件
	objConf存放对象配置文件

2.pmcfg.properties文件配置基本的程序内容，pool为数据库连接信息
	confType可以配置是从数据库中读取配置还是从xml文件中读取.
	
3.objConf文件夹下存放每个omc对应的配置
		
5.程序参数说明 
	参数1：厂家ID
	参数2：需要解析的XML文件路径
	参数3：OMCID
	参数4：补采表示，1表示正常采集，2表示手动补采。补采时将使用补采的表名配置
	
6.执行方法
	java -jar ltecmparse.jar %1 %2 %3 %4
	
7.如果需要增加omc采集，可在objConf下增加配置文件，然后修改conf/pmcfg.properties文件中对应设置。

8.如果需要增加采集对象以及属性，可修改rel下的对应配置文件，按照第四项的说明进行配置即可。
		
9.已知常量说明
	file_name：对应对象所在的文件名
	key-xmlpath：对应对象所在文件路径
	i_factory_id：调用程序是传入的厂家ID
	dtm_repdate：调用时传入的时间
	i_omc_id：调用时传入的OMCID
	DateTime：对应文件中的DateTime，已替换掉T和截取+8:00
	BeginTime：对应文件中的BeginTime 已替换掉T和截取+8:00
	EndTime：对应文件中的EndTime 已替换掉T和截取+8:00
	obj-Dn:对应对象完整的DN字符串
	obj-UserLabel:对应对象的属性UserLabel
	Dn-XXX:对应c节点Dn属性字符串中对应属性的值，例如：SubNetwork=Nokia-101802,ManagedElement=MRBTS-233679,AntennaFunction=ANTL-7
		则会存放 Dn-SubNetwork，Dn-ManagedElement,Dn-AntennaFunction 其他以此类推。


xml配置方式
objConf下配置文件配置说明：
	每一个<Object>对应一个需要解析的对象
	<ObjectType>代表需要解析的XML数据文件中的对象名
	<tablename>代表此对象对应的数据库表明
	<temp_tablename>配置补采时用的表名
	<FieldName>代表需要解析的字段
	<N>代表各个字段的类型及属性
		属性说明：
			type：此字段对应的数据库类型 string(字符),int(整型),number(数字),date(日期),datetime
			tablecolumn：数据库字段名称
			length：数据库的字段长度，对string有效，不配置则不限制
			format：数据内容的格式，对date有效，不配置则使用默认格式
			regex：正则表达式，用于替换字符串，此字符串会不做处理直接用于replaceAll方法，不配置则表示不使用正则表达式替换。
	


数据库配置
1.object_table 对象表，表示要解析的对象以及对应的数据表，其中字段CONFIGNAME为配置名称
	在pmcfg配置文件中当confType=db时，需要为omc配置此对象名称，表示此omc需要解析哪些对象。
	程序初始会根据配置的名称查询数据表object_table的数据记录。
	其中字段SUB_TABLENAME用于保存分项指标的表名
	TARGETNAMEFIELD开头的表示存储含有分项指标名称的字段
	SUBTARGETNAMEFIELD开头的字段表示存储分项指标名称的字段
	TARGETVALUEFIELD开头的字段表示存储分项指标值的字段
	
	SUB_TABLENAME 如果此字段为空则表示没有字表，不需要采集分项指标，如果不为空则其余相关字段也必须有配置信息。
	
2.field_table字段表，即需要解析的对象的字段、属性对应关系以及类型等信息。

3.subtable_field 分项指标表(字表),其他字段的存储配置

4.subtable_target 表示有哪些指标需要存储分项指标

注：field_table、subtable_field、subtable_target表中的OBJECT_ID均指向object_table表的ID
	,所以object_table不能为空且必须唯一。
	 每个对象对应的表可在properties文件中制定，但表结构必须一致。
	 
		