import pandas as pd
import numpy as np

class Data:
	
	list_col=None
	ser_dtype=None
	ncolumns=None
	nrows=None
	matrix=None
	pk_dic={}
	primary_key=[]
	
	
	def __init__ (self,df):
		Data.list_col=list(df.columns)
		Data.ser_dtype=df.dtypes
		Data.nrows,Data.ncolumns=df.shape
		Data.matrix=np.zeros((Data.ncolumns,Data.ncolumns))
		

	def numeric_col(self,s):
		minimum=s.min()
		maximum=s.max()
		mean=s.mean()
		std=s.std()
		uniq=s.unique()
		uniq_val=len(uniq)
		if(uniq_val==Data.nrows):
			Data.primary_key.append(s.name)
		Data.pk_dic[uniq_val]=s.name
		print "possible values : ",uniq_val
		if(uniq_val<=10):
			print "possible values are : ",uniq
		print "mean : ",mean
		print "std : ",std
		print "maximum : ",maximum
		print "minimum : ",minimum
		return





	def string_col(self,s):
		uniq=s.unique()
		uniq_val=len(uniq)
		if(uniq_val==Data.nrows):
			Data.primary_key.append(s.name)
		Data.pk_dic[uniq_val]=s.name
		max_len=0
		for i in s:
			i=str(i)
			n=len(i)
			if(n>max_len):
				max_len=n
		print "possible values : ",uniq_val
		if(uniq_val<=10):
			print "possible values are : ",uniq
		print "Max length of the string : ",max_len
		return



	def datetime_col(self,s):
		uniq=s.unique()
		uniq_val=len(uniq)
		if(uniq_val==Data.nrows):
			Data. primary_key.append(s.name)
		Data.pk_dic[uniq_val]=s.name
		print "possible values : ",uniq_val
		if(uniq_val<=10):
			print "possible values are : ",uniq
		return



	def prop_data(self,df):
		for i in Data.list_col:
			data_type=Data.ser_dtype[i]
			if(data_type == "object"):
				data_type="string"
			if(data_type == "int64"  or data_type == "float64"):
				print "column name : ",i
				print "data type : ",data_type
				self.numeric_col(df[i])
				print "\n"
			elif (data_type == "string"):
				print "column name : ",i
				print "data type : ",data_type
				self.string_col(df[i])
				print "\n"
			else:
				print "column name : ",i
				print "data type : ",data_type
				self.datetime_col(df[i])	
				print "\n"
		return


	def relation_main(self,df):
		m=Data.matrix
		for i in range(0,Data.ncolumns):
			for j in range(0,Data.ncolumns):
				if(i!=j):
					col_1=df[Data.list_col[i]]
					col_2=df[Data.list_col[j]]
					dic={}
					for a in range(0,Data.nrows):
						if(col_1[a] not in dic):
							dic[col_1[a]]=col_2[a]
							if(a==Data.nrows-1):			
								m[i][j]=11
							
						else:
							if(dic[col_1[a]]!=col_2[a]):
								m[i][j]=12
								break
		return

	

	def print_rel(self,m):
		n=Data.ncolumns
		for i in range(0,n):
			for j in range(0,n):
				if(i!=j):
					if(m[i][j]==float(11) and m[j][i]==float(11)):
						print Data.list_col[i]," and ",Data.list_col[j]," has one-one relationship"
					if(m[i][j]==float(12) and m[j][i]==float(11)):
						print Data.list_col[i]," and ",Data.list_col[j]," has one-many relationship"
					if(m[i][j]==float(11) and m[j][i]==float(12)):
						print Data.list_col[i]," and ",Data.list_col[j]," has many-one relationship"
					if(m[i][j]==float(12) and m[j][i]==float(12)):
						print Data.list_col[i]," and ",Data.list_col[j]," has both  relationship"
		
	
	
	
	def prim_used(self,l,df):
		subset=df[l]
		tuples = [tuple(x) for x in subset.values]
		if(len(tuples)==Data.nrows):
			return True
		else:
			return False


	
	def primarykey(self,df):
		print "hmm"
		if(len(Data.primary_key)==0):
			sort_pk=Data.pk_dic.keys()
			n=len(sort_pk)
			sort_pk.sort()
			sort_pk.reverse()
			print "hmm"
			print sort_pk
			for i in range(0,n-1):
				for j in range(i+1,n):
					l=[]
					l.append(Data.pk_dic[sort_pk[i]])
					l.append(Data.pk_dic[sort_pk[j]])
					if(self.prim_used(l,df)==True):
						t=tuple(l)
						Data.primary_key.append(t)
			if(len(Data.primary_key)==0):
				for i in range(0,Data.ncolumns-2):
					for j in range(i+1,Data.ncolumns-1):
						for k in range(j+1,Data.ncolumns):
							l=[]
							l.append(Data.pk_dic[sort_pk[i]])
							l.append(Data.pk_dic[sort_pk[j]])
							l.append(Data.pk_dic[sort_pk[k]])
							if(self.prim_used(l,df)==True):
								t=tuple(l)
								Data.primary_key.append(t)
			if(len(Data.primary_key)==0):				
				print "not yet found"
						
					
	
						
		
					

df=pd.read_csv("ibmproject.csv",parse_dates= ['Ticket Number','Assigned','Status','Client','Category','Severity','Date Created','Date Last Modified','Response Date','Date Closed','Time Spent (min)','Parent Ticket'])
ob=Data(df)
print Data.list_col
print Data.ser_dtype
print Data.nrows
print Data.ncolumns
print Data.matrix
ob.prop_data(df)
ob.relation_main(df)
print Data.matrix
ob.print_rel(Data.matrix)
print Data.primary_key
print Data.pk_dic
ob.primarykey(df)
print Data.primary_key

