import pandas as pd
import numpy as np
from datetime import datetime
from messytables import CSVTableSet, type_guess,types_processor, headers_guess, headers_processor,offset_processor, any_tableset
import itertools as iter
import csv

class Data:

	#basic properties related to csv data
	list_col=None      #list of columns
	ser_dtype=None	   #datatypes of columns detected by pandas
	ncolumns=None	   #number of columns in the csv table
	nrows=None		   #number of rows in the csv table
	matrix=None		   #Relation Matrix

	#Initialise the object(Constructer)
	def __init__ (self,df):
		Data.list_col=list(df.columns)
		Data.ser_dtype=df.dtypes
		Data.nrows,Data.ncolumns=df.shape
		Data.matrix=np.zeros((Data.ncolumns,Data.ncolumns))

#Part 2:Finding various properties related to the columns
	#Done Finding Mean,maximum,minimum,std using inbuilt dataframe functions
	def numeric_col(self,s):
		minimum=s.min()
		maximum=s.max()
		mean=s.mean()
		std=s.std()
		uniq=s.unique()
		uniq_val=len(uniq)
		print "possible values : ",uniq_val
		if(uniq_val<=10):
			print "possible values are : ",uniq
		print "mean : ",mean
		print "std : ",std
		print "maximum : ",maximum
		print "minimum : ",minimum
		return

	#String column(datatype:String)
	def string_col(self,s):
		uniq=s.unique()
		uniq_val=len(uniq)
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
	#datetime column(datatype:datetime)
	def datetime_col(self,s):
		uniq=s.unique()
		uniq_val=len(uniq)
		print "possible values : ",uniq_val
		if(uniq_val<=10):
			print "possible values are : ",uniq
		return

	#general column(none of the methodised types)
	def genric_col(self,s):
		uniq=s.unique()
		uniq_val=len(uniq)
		print "possible values : ",uniq_val
		if(uniq_val<=10):
			print "possible values are : ",uniq
		return

	#bool Column Datatype(Bool)
	def bool_col(self,s):
		uniq=s.unique()
		uniq_val=len(uniq)
		print "possible values : ",uniq_val
		print "possible values are : ",uniq
		return

	#main funtion that calls all other type functions like numeric etc
	def prop_data(self,df,types):
		for i in xrange(0,len(Data.list_col)):
			data_type=types[i]
			if(str(data_type) == "Integer"  or str(data_type) == "Decimal"):
				print "column name : ",i
				print "data type : ",str(data_type)
				self.numeric_col(df[Data.list_col[i]])
				print "\n"
			elif (str(data_type) == "String"):
				print "column name : ",i
				print "data type : ",str(data_type)
				self.string_col(df[Data.list_col[i]])
				print "\n"
			elif (str(data_type) == "Bool"):
				print "column name : ",i
				print "data type : ",str(data_type)
				self.bool_col(df[Data.list_col[i]])
				print "\n"
			elif (data_type == datetime):
				print "column name : ",i
				print "data type : ",str(data_type)
				self.datetime_col(df[Data.list_col[i]])
				print "\n"
			else:
				print "column name : ",i
				print "data type : ",str(data_type)
				self.genric_col(df[Data.list_col[i]])
				print "\n"
		return

	#main function that Finds the relation between a pair of columns
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
	#prints the relation that we find in rel_main
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


tests = [
    # (Type, Test)
	(datetime, lambda value: datetime.strptime(value, "%Y")),
	(datetime, lambda value: datetime.strptime(value, "%y")),
    (datetime, lambda value: datetime.strptime(value, "%Y/%m/%d")),
    (datetime, lambda value: datetime.strptime(value, "%Y-%m-%d")),
    (datetime, lambda value: datetime.strptime(value, "%y/%m/%d")),
    (datetime, lambda value: datetime.strptime(value, "%y-%m-%d")),
    (datetime, lambda value: datetime.strptime(value, "%d-%m-%Y")),
    (datetime, lambda value: datetime.strptime(value, "%d/%m/%Y")),
    (datetime, lambda value: datetime.strptime(value, "%d/%m/%y")),
    (datetime, lambda value: datetime.strptime(value, "%d-%m-%y")),
    (datetime, lambda value: datetime.strptime(value, "%m-%d-%Y")),
    (datetime, lambda value: datetime.strptime(value, "%m/%d/%Y")),
    (datetime, lambda value: datetime.strptime(value, "%m-%d-%y")),
    (datetime, lambda value: datetime.strptime(value, "%m/%d/%y")),
    (datetime, lambda value: datetime.strptime(value, "%Y/%m/%d %H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%Y-%m-%d %H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%d-%m-%Y %H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%d/%m/%Y %H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%d/%m/%y %H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%d-%m-%y %H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%m-%d-%Y %H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%m/%d/%Y %H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%Y/%m/%d-%H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%Y-%m-%d-%H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%d-%m-%Y-%H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%d/%m/%Y-%H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%d/%m/%y-%H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%d-%m-%y-%H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%m-%d-%Y-%H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%m/%d/%Y-%H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%Y/%m/%d.%H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%Y-%m-%d.%H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%d-%m-%Y.%H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%d/%m/%Y.%H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%d/%m/%y.%H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%d-%m-%y.%H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%m-%d-%Y.%H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%m/%d/%Y.%H:%M:%S")),
    (datetime, lambda value: datetime.strptime(value, "%Y/%m/%d.%H.%M.%S")),
    (datetime, lambda value: datetime.strptime(value, "%Y-%m-%d.%H.%M.%S")),
    (datetime, lambda value: datetime.strptime(value, "%d-%m-%Y.%H.%M.%S")),
    (datetime, lambda value: datetime.strptime(value, "%d/%m/%Y.%H.%M.%S")),
    (datetime, lambda value: datetime.strptime(value, "%d/%m/%y.%H.%M.%S")),
    (datetime, lambda value: datetime.strptime(value, "%d-%m-%y.%H.%M.%S")),
    (datetime, lambda value: datetime.strptime(value, "%m-%d-%Y.%H.%M.%S")),
    (datetime, lambda value: datetime.strptime(value, "%m/%d/%Y.%H.%M.%S")),
    (datetime, lambda value: datetime.strptime(value, "%Y/%m/%d-%H.%M.%S")),
    (datetime, lambda value: datetime.strptime(value, "%Y-%m-%d-%H.%M.%S")),
    (datetime, lambda value: datetime.strptime(value, "%d-%m-%Y-%H.%M.%S")),
    (datetime, lambda value: datetime.strptime(value, "%d/%m/%Y-%H.%M.%S")),
    (datetime, lambda value: datetime.strptime(value, "%d/%m/%y-%H.%M.%S")),
    (datetime, lambda value: datetime.strptime(value, "%d-%m-%y-%H.%M.%S")),
    (datetime, lambda value: datetime.strptime(value, "%m-%d-%Y-%H.%M.%S")),
    (datetime, lambda value: datetime.strptime(value, "%m.%d.%Y.%H.%M.%S")),
	(datetime, lambda value: datetime.strptime(value, "%m.%d.%Y.%H.%M.%S")),
	(datetime, lambda value: datetime.strptime(value, "%m.%d.%Y.%H.%M.%S"))
]
#type detection for datetime formats,for tests Tests array defined above is used
def getType(value):
	for typ, test in tests:
	    try:
	        test(value)
	        return typ
	    except ValueError:
	        continue
	# No match
	return str

#CASE:1 from Messy Tables #specify The Path where the file is
print "Give the path to the file(give absolute/relative path to the file)"
filename = raw_input()
datafile = file(filename)
fh = open(filename, 'r')
table_set = CSVTableSet(fh)

# A table set is a collection of tables:
row_set = table_set.tables[0]

# guess header names and the offset of the header:
offset, headers = headers_guess(row_set.sample)
row_set.register_processor(headers_processor(headers))

# add one to begin with content, not the header:
row_set.register_processor(offset_processor(offset + 1))

# guess column types:
types = type_guess(row_set.sample, strict=True)

# and tell the row set to apply these types to
# each row when traversing the i:
row_set.register_processor(types_processor(types))

def update_datetypes(dataFrame,ListColumns,rw):
	for x in xrange(0,len(ListColumns)):
		datet = False
		tempList = dataFrame[ListColumns[x]].tolist()
		for i in xrange(1,rw):
			if(not type(tempList[i]) == str):
				break
			if(getType(tempList[i]) == datetime):
				datet = True
				break
		if(datet):
			types[x] = datetime

#creating dataframe
df=pd.read_csv(datafile)
ob=Data(df)

#update the types
update_datetypes(df,Data.list_col,Data.nrows)

print
print "Guessed datatypes of tables:---->"
print

for i in xrange(0,len(types)):
	print Data.list_col[i],"------------->",types[i]

print
print
ob.prop_data(df,types)
ob.relation_main(df)
print
print Data.matrix
print
ob.print_rel(Data.matrix)
