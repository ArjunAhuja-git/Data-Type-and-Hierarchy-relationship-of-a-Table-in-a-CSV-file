import csv
import pandas as pd
import numpy as np
from messytables import CSVTableSet, type_guess,types_processor, headers_guess, headers_processor,offset_processor, any_tableset


class relation(object):
    """docstring for relation"""
    def __init__(self):
        self.one_to_one = []
        self.one_to_many = []
        self.col_many_col = []
        self.col_count = 0


#INFORMATION ON CELL DATA DATE OBJECT STILL UNCLEAR! -----open(inference 1)------

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

# the datatype of each column as identified:
print types

print headers


#INFORMATION ON CELL DATA DATE OBJECT STILL UNCLEAR! -----close------

#reading csv data ------finding realtions between various columns(inference 2)-------
x =	pd.read_csv(datafile)


#list of column header values!
header_value = list(x.columns.values)

print header_value

unique = [False]*len(header_value)
number_of_unique_values = [0]*len(header_value)
counter = 0 #just an i
counter_unique = 0 #number of unique coumns

col_list = [];

for header in header_value:
    converted_list = x[header].tolist()
    if counter == 0:
        col_list = converted_list
    elif counter == 1:
        col_list = [col_list,converted_list]
    else:
        col_list.append(converted_list)
    set_converted_list = set(converted_list)
    if(len(set_converted_list) == len(converted_list)):
        unique[counter] = True
        counter_unique = counter_unique + 1
    counter = counter+1

#print counter_unique
#print header_value

rel_ob_ar = [] #relation object array
col_set_length = []

for x2 in xrange(0,len(col_list)):
    rel_ob_ar.append(relation())
    col_set_length.append(len(set(col_list[x2])))

#-----------------hierarchy (idea value of column x should be present in column y and number of values of y should be less than x)
for i in xrange(0,len(col_list)):
    for j in xrange(i+1,len(col_list)):
        if i!=j:
            if col_set_length[i] > col_set_length[j]:
                #print set(col_list[i])
                #print set(col_list[j])
                #print "one-many relation found"
                rel_ob_ar[i].one_to_many.append(j)
                #check for one to one and one to many here! Done!
            elif col_set_length[i] == col_set_length[j]:
                var_Array = []
                for x in xrange(0,len(col_list[i])):
                    if x==0:
                        var_Array = [col_list[i][x],col_list[j][x]]
                    elif x==1:
                        var_Array = [var_Array,[col_list[i][x],col_list[j][x]]]
                    else:
                        var_Array.append([col_list[i][x],col_list[j][x]])
                len_tup = len(set(tuple(g) for g in var_Array))
                if len_tup == col_set_length[i]:
                    #print set(col_list[i])
                    #print set(col_list[j])
                    #print "one-one relation found"
                    rel_ob_ar[i].one_to_one.append(j)
                    rel_ob_ar[j].one_to_one.append(i)
                else:
                    #print set(col_list[i])
                    #print set(col_list[j])
                    #print "one-many relation found"
                    rel_ob_ar[i].one_to_many.append(j)
                    rel_ob_ar[j].one_to_many.append(i)
            else :
                rel_ob_ar[j].one_to_many.append(i)


#---------------------------Hierarchy close---------------------------------#

print "heirarchy close"

#---------------------------Column to many Column Relation------------------#
for ob in xrange(0,len(rel_ob_ar)):
    for i in xrange(0,len(rel_ob_ar[ob].one_to_many)):
        lst_var = [rel_ob_ar[ob].one_to_many[i]]
        for i_2 in xrange(0,len(rel_ob_ar[rel_ob_ar[ob].one_to_many[i]].one_to_one)):
            lst_var.append(rel_ob_ar[rel_ob_ar[ob].one_to_many[i]].one_to_one[i_2])
            if rel_ob_ar[ob].col_count == 0:
                rel_ob_ar[ob].col_many_col = lst_var
                rel_ob_ar[ob].col_count = rel_ob_ar[ob].col_count+1
            elif rel_ob_ar[ob].col_count == 1:
                rel_ob_ar[ob].col_many_col = [rel_ob_ar[ob].col_many_col,lst_var]
                rel_ob_ar[ob].col_count = rel_ob_ar[ob].col_count+1
            else:
                rel_ob_ar[ob].col_many_col.append(lst_var)
                rel_ob_ar[ob].col_count = rel_ob_ar[ob].col_count+1
    for i in xrange(0,len(rel_ob_ar[ob].one_to_one)):
        for i_2 in xrange(0,len(rel_ob_ar[rel_ob_ar[ob].one_to_one[i]].one_to_one)):
            lst_var.append(rel_ob_ar[rel_ob_ar[ob].one_to_one[i]].one_to_one[i_2])
            if rel_ob_ar[ob].col_count == 0:
                rel_ob_ar[ob].col_many_col = [lst_var]
                rel_ob_ar[ob].col_count = rel_ob_ar[ob].col_count+1
            else:
                rel_ob_ar[ob].col_many_col.append([lst_var])
                rel_ob_ar[ob].col_count = rel_ob_ar[ob].col_count+1


#---------------------------Column to many Column close---------------------#

print "removing duplicates"

#---------------------------removing duplicates from the relation object array col_many_col---------#

for ob in xrange(0,len(rel_ob_ar)):
    #print rel_ob_ar[ob].col_many_col
    #rel_ob_ar[ob].col_many_col = set(tuple(g) for g in rel_ob_ar[ob].col_many_col)
    rel_ob_ar[ob].one_to_one = set(rel_ob_ar[ob].one_to_one)
    rel_ob_ar[ob].one_to_many = set(rel_ob_ar[ob].one_to_many)

#---------------------------removing duplicates from the relation object array col_many_col---------#

#TODO's still to confirm the format of date
#have to take care of multiple tables
#still to make inference on the date and int as to values of one table is greater than another!
#still to check with sir the can None/empty occuring many time can be a one to many relation