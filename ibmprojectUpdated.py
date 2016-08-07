#TODOS:
#TASK 1:Find the Types of Data columns (done date format still to be done!)
#TASK 2:Find Functional Dependencies(Doing Now)

import csv
import pandas as pd
import numpy as np
from messytables import CSVTableSet, type_guess,types_processor, headers_guess, headers_processor,offset_processor, any_tableset
import itertools as iter
#-------All functions Used------------#

from itertools import combinations
#Input is a List and Output is a list of all possible combinations,We have to Ignore the first and Last Combination,empty and all
def GetAllCombinations(input): #to get all combinations of columns may make the code a bit lengthy but is required
	return sum([map(list, combinations(input, i)) for i in range(len(input) + 1)], [])

#----finds Number of Missing Values(NaN in each column)-----#
def num_missing(x):
  return sum(x.isnull())

#testing datetime formats

from datetime import datetime

fmts = ('%Y','%b %d, %Y','%b %d, %Y','%B %d, %Y','%B %d %Y','%m/%d/%Y','%m-%d-%Y','%m/%d/%y','%m-%d-%y','%y-%m-%d','%Y-%m-%d','%Y/%m/%d','%y/%m/%d','%b %Y','%B%Y','%b %d,%Y')
tests = [
    # (Type, Test)
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
    (datetime, lambda value: datetime.strptime(value, "%m/%d/%Y-%H.%M.%S"))
]

def getType(value):
	for typ, test in tests:
	    try:
	        test(value)
	        return typ
	    except ValueError:
	        continue
	# No match
	return str

#--------second get the column types using messyTables---------#

#CASE:1 from Messy Tables

datafile = file('ibmproject.csv')
fh = open('ibmproject.csv', 'r')
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
		

#-------doing rest of operations using pandas------#
dataFrame =	pd.read_csv(datafile)
ListColumns = list(dataFrame.columns)
rw,col = dataFrame.shape                           #number of rows,number of colmuns
ListOfFunctionalDependecies = {}
ListOfCombinationsOfColumns = GetAllCombinations(ListColumns)
for_primary_key_and_file_breaking = [[]]*(len(ListOfCombinationsOfColumns))
candidate_keys = []
one_to_one_relation = []
many_to_one_relation = []
super_keys = []
StoringAllRelationsInADictionaryOfTuples = {}

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

for i in xrange(1,len(ListOfCombinationsOfColumns)-1): #taking all 2^n-2 combinations of columns
	col_2 = []
	for y in xrange(0,len(ListOfCombinationsOfColumns[i])):
		if(y==0):
			col_2 = [dataFrame[ListOfCombinationsOfColumns[i][y]].tolist()]
		else:
			col_2.append(dataFrame[ListOfCombinationsOfColumns[i][y]].tolist())
	col_2 = map(list, zip(*col_2))               #Transpose of original Col_2
	col_2_tuples = [tuple(l) for l in col_2]	 #Taking a list of columns and tranforming their rows into tuples
	#Tuple to Compare
	for_primary_key_and_file_breaking[i] = []
	for j in xrange(0,len(ListColumns)):
#		if(ListColumns[j] not in ListOfCombinationsOfColumns[i]): not requires as XAB doesn't mean XAB->X
		dic = {}
		dic2 = {}
		col_1 = dataFrame[ListColumns[j]].tolist()
		flag = False
		many_to_one = False
		for x in xrange(0,rw):
			if(col_2_tuples[x] not in dic):
				dic[col_2_tuples[x]] = col_1[x]
			elif(dic[col_2_tuples[x]] != col_1[x]):
				flag = True
				break
			if(col_1[x] not in dic2):
				dic2[col_1[x]] = col_2_tuples[x]
			elif(dic2[col_1[x]] != col_2_tuples[x]):
				many_to_one = True
		if(not flag):
			for_primary_key_and_file_breaking[i].append(ListColumns[j])
			if(len(for_primary_key_and_file_breaking[i]) == len(ListColumns)):
				super_keys.append(ListOfCombinationsOfColumns[i])
			if(many_to_one):
				many_to_one_relation.append([ListOfCombinationsOfColumns[i],[ListColumns[j]]])
			else:
				one_to_one_relation.append([ListOfCombinationsOfColumns[i],[ListColumns[j]]])
	StoringAllRelationsInADictionaryOfTuples[tuple(ListOfCombinationsOfColumns[i])] = for_primary_key_and_file_breaking[i]


#Finished Storing Function Dependencies in One to One and Many to One Lists! #:-( taking a lot of time to ru)

primary_key = None #Defining The Primary Key To Be Null Initially

#----finds Number of Missing Values(NaN in each column)-----#
Column_missing_values = dataFrame.apply(num_missing, axis=0) 

#Selecting the Primary Key From the above Super Keys(not exactly super key still have to care of NULL values)
for x in xrange(0,len(super_keys)):
	CanBe = True
	for j in xrange(0,len(super_keys[x])):
		if(Column_missing_values[super_keys[x][j]] != 0):
			CanBe = False
			break
	if(CanBe):
		primary_key = super_keys[x]
		break
	else:
		CanBe = True

if(primary_key == None):
	print "No Primary Key Is Found"
else:
	print primary_key

#Now for the Last and Final Part Normalisation!
AllCombinationsOfPrimaryKeyFor2NFNormalisation = GetAllCombinations(primary_key)
ColumnsToRemove = []
ExtraTables = []
for i in xrange(0,len(ListColumns)):
	for x in xrange(1,len(AllCombinationsOfPrimaryKeyFor2NFNormalisation)-1):
		if(ListColumns[i] not in AllCombinationsOfPrimaryKeyFor2NFNormalisation[x]):
			if(ListColumns[i] in StoringAllRelationsInADictionaryOfTuples[tuple(AllCombinationsOfPrimaryKeyFor2NFNormalisation[x])]):
				ColumnsToRemove.append(ListColumns[i])
				A = list(AllCombinationsOfPrimaryKeyFor2NFNormalisation[x])
				A.append(ListColumns[i])
				ExtraTables.append(A)
				break

#TODO remove Columns from MAIN TABLE!
#3nf normalisation
ColumnsLeft = [x for x in ListColumns if x not in ColumnsToRemove] #updated main table after 2nf normalisation
#update list column Removed

ColumnsExceptPrimaryKeyNow = [x for x in ColumnsLeft if x not in primary_key]

#3nf Relations
ExtraTablesAfter3NF = []
ColumnsToRemoveAfter3NF = []
AllCombinationsOfNonPrimaryKeyFor3NFNormalisation = GetAllCombinations(ColumnsExceptPrimaryKeyNow)

for x in AllCombinationsOfNonPrimaryKeyFor3NFNormalisation:
	print x
for i in xrange(0,len(ListColumns)):
	for x in xrange(1,len(AllCombinationsOfNonPrimaryKeyFor3NFNormalisation)-1):
		if(ListColumns[i] not in AllCombinationsOfNonPrimaryKeyFor3NFNormalisation[x]):
			if(AllCombinationsOfNonPrimaryKeyFor3NFNormalisation[x] not in super_keys):
				if(ListColumns[i] in StoringAllRelationsInADictionaryOfTuples[tuple(AllCombinationsOfNonPrimaryKeyFor3NFNormalisation[x])]):
					ColumnsToRemoveAfter3NF.append(ListColumns[i])
					A = list(AllCombinationsOfNonPrimaryKeyFor3NFNormalisation[x])
					A.append(ListColumns[i])
					ExtraTables.append(A)
					break

print ExtraTables
print ColumnsToRemoveAfter3NF

ColumnsLeft = [x for x in ListColumns if x not in ColumnsToRemoveAfter3NF]







					