1.conf�ļ��д�Ż��������ļ�������cfg��pool�ļ���
	lib��ű�Ҫ�İ�
	log�����־�ļ�
	objConf��Ŷ��������ļ�

2.pmcfg.properties�ļ����û����ĳ������ݣ�poolΪ���ݿ�������Ϣ
	confType���������Ǵ����ݿ��ж�ȡ���û��Ǵ�xml�ļ��ж�ȡ.
	
3.objConf�ļ����´��ÿ��omc��Ӧ������
		
5.�������˵�� 
	����1������ID
	����2����Ҫ������XML�ļ�·��
	����3��OMCID
	����4�����ɱ�ʾ��1��ʾ�����ɼ���2��ʾ�ֶ����ɡ�����ʱ��ʹ�ò��ɵı�������
	
6.ִ�з���
	java -jar ltecmparse.jar %1 %2 %3 %4
	
7.�����Ҫ����omc�ɼ�������objConf�����������ļ���Ȼ���޸�conf/pmcfg.properties�ļ��ж�Ӧ���á�

8.�����Ҫ���Ӳɼ������Լ����ԣ����޸�rel�µĶ�Ӧ�����ļ������յ������˵���������ü��ɡ�
		
9.��֪����˵��
	file_name����Ӧ�������ڵ��ļ���
	key-xmlpath����Ӧ���������ļ�·��
	i_factory_id�����ó����Ǵ���ĳ���ID
	dtm_repdate������ʱ�����ʱ��
	i_omc_id������ʱ�����OMCID
	DateTime����Ӧ�ļ��е�DateTime�����滻��T�ͽ�ȡ+8:00
	BeginTime����Ӧ�ļ��е�BeginTime ���滻��T�ͽ�ȡ+8:00
	EndTime����Ӧ�ļ��е�EndTime ���滻��T�ͽ�ȡ+8:00
	obj-Dn:��Ӧ����������DN�ַ���
	obj-UserLabel:��Ӧ���������UserLabel
	Dn-XXX:��Ӧc�ڵ�Dn�����ַ����ж�Ӧ���Ե�ֵ�����磺SubNetwork=Nokia-101802,ManagedElement=MRBTS-233679,AntennaFunction=ANTL-7
		����� Dn-SubNetwork��Dn-ManagedElement,Dn-AntennaFunction �����Դ����ơ�


xml���÷�ʽ
objConf�������ļ�����˵����
	ÿһ��<Object>��Ӧһ����Ҫ�����Ķ���
	<ObjectType>������Ҫ������XML�����ļ��еĶ�����
	<tablename>����˶����Ӧ�����ݿ����
	<temp_tablename>���ò���ʱ�õı���
	<FieldName>������Ҫ�������ֶ�
	<N>��������ֶε����ͼ�����
		����˵����
			type�����ֶζ�Ӧ�����ݿ����� string(�ַ�),int(����),number(����),date(����),datetime
			tablecolumn�����ݿ��ֶ�����
			length�����ݿ���ֶγ��ȣ���string��Ч��������������
			format���������ݵĸ�ʽ����date��Ч����������ʹ��Ĭ�ϸ�ʽ
			regex��������ʽ�������滻�ַ��������ַ����᲻������ֱ������replaceAll���������������ʾ��ʹ��������ʽ�滻��
	


���ݿ�����
1.object_table �������ʾҪ�����Ķ����Լ���Ӧ�����ݱ������ֶ�CONFIGNAMEΪ��������
	��pmcfg�����ļ��е�confType=dbʱ����ҪΪomc���ô˶������ƣ���ʾ��omc��Ҫ������Щ����
	�����ʼ��������õ����Ʋ�ѯ���ݱ�object_table�����ݼ�¼��
	�����ֶ�SUB_TABLENAME���ڱ������ָ��ı���
	TARGETNAMEFIELD��ͷ�ı�ʾ�洢���з���ָ�����Ƶ��ֶ�
	SUBTARGETNAMEFIELD��ͷ���ֶα�ʾ�洢����ָ�����Ƶ��ֶ�
	TARGETVALUEFIELD��ͷ���ֶα�ʾ�洢����ָ��ֵ���ֶ�
	
	SUB_TABLENAME ������ֶ�Ϊ�����ʾû���ֱ�����Ҫ�ɼ�����ָ�꣬�����Ϊ������������ֶ�Ҳ������������Ϣ��
	
2.field_table�ֶα�����Ҫ�����Ķ�����ֶΡ����Զ�Ӧ��ϵ�Լ����͵���Ϣ��

3.subtable_field ����ָ���(�ֱ�),�����ֶεĴ洢����

4.subtable_target ��ʾ����Щָ����Ҫ�洢����ָ��

ע��field_table��subtable_field��subtable_target���е�OBJECT_ID��ָ��object_table���ID
	,����object_table����Ϊ���ұ���Ψһ��
	 ÿ�������Ӧ�ı����properties�ļ����ƶ�������ṹ����һ�¡�
	 
		