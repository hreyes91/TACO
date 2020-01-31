
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul 13 12:36:34 2019

@author: reyes-gonzalez
"""

import numpy as np
import pandas as pd
import argparse

import time
start = time.time()


def argparser():
    ''' Get arguments from the command line '''
    
    parser = argparse.ArgumentParser(description="codeTACO command line tool.")

    parser.add_argument('-f', '--filename', required=True,
            help=" Data set of populated SRs for each event." )
    
    parser.add_argument('-o', '--outputname', type=str, default='independcy_matrix.txt',
            help="Output: Matrix of independency among analyses/signal regions. Default: independcy_matrix.txt " )
    
    parser.add_argument('-c', '--ncopies', type=int, default=100,
        help="Number of copies of the data for bootsprapping. Default 100")
    
    parser.add_argument('-k', '--chunksize', type=int, default=100000,
        help="Size of each chunk (in number of rows) of input data. Default 100000.")
    
    parser.add_argument('-t', '--threshold', type=float, default=0.01,
        help="correlation coefficient threshold of independency. Default 0.01.")
    
    
    args=parser.parse_args()
    
    return args
    
    
    
    
    
    
    

def header(name_of_file):
    '''Read and return the header of the dataset '''


    column_names=list(pd.read_csv(name_of_file, nrows=1,sep=' ').columns)
    #firstline=open(name_of_file).readline().rstrip()
    #column_names=firstline.split(' ')
    #firstline.close()

    return column_names


def new_row(name_of_file,column_names):
    '''Creates a new  row filled with zeros to be replaced with the sum of weights'''
    zeros_row=[]
    
    for SR in range(len(column_names)):
        zeros_row.append(0)
    

    return zeros_row


def chunk_down(name_of_file,new_line,column_names,chunksize):
    '''Reads the original dataframe by chunks, gives a weight to each event, and sums weigth1*event1+weigth2*event2...+weigthN*eventN.
    Returns a new row for the table of bootstrapped events.  '''
    

    
    for chunk in pd.read_csv(name_of_file, chunksize=chunksize,sep=' '):
        
      
       
        for index, row in chunk.iterrows():
            row=row.tolist() #[0].split(' ')

 
            #summing reweightedline
            weigth=int(np.random.poisson(1,1))
            reweigth_row= [int(i) *weigth  for i in row]

            new_line=[x + y for x, y in zip(new_line, reweigth_row)]
            

            
    new_line=pd.DataFrame([new_line],columns=column_names)
        
        
    return new_line    
        
  





def build_weights_dataset(ncopies,column_names):
    
    bootstrapped_table=pd.DataFrame(columns=column_names)
    
    list_of_lines=[]

    for copy in range(ncopies):    
        
        if copy%10==0:
            print(copy,'copies done out of',ncopies)
        row_zeros=new_row(args.filename,column_names)
        new_line=chunk_down(args.filename,row_zeros,column_names,args.chunksize)
        list_of_lines.append(new_line)
        
        
    bootstrapped_table=pd.concat(list_of_lines,ignore_index=True)  
    
    
    #bootstrapped_table.to_csv('boot_table.txt')
    return bootstrapped_table
          

def correlation_matrix(bootstrapped_table):
    ''' Computes the correlation matrix'''    
    corr_matrix=bootstrapped_table.corr()    
    #corr_matrix.to_csv('correlation_matrix_all.txt')
    return corr_matrix
    

def independency_matrix(corr_matrix,threshold,outputname):
    '''Builds the independancy matrix form the correlation matrix and a independency threshold'''
    
    ind_matrix=corr_matrix

    for row in column_names:
        for column in column_names: 
            if abs(ind_matrix.at[row,column])>threshold:
                ind_matrix.loc[[row],[column]]=int(1)
            elif abs(ind_matrix.at[row,column])<=threshold:             
               ind_matrix.loc[[row],[column]]=int(0)              

    
    ind_matrix.to_csv(outputname)
    
    return ind_matrix







args=argparser()            
column_names=header(args.filename)
bootstrapped_table=build_weights_dataset(args.ncopies,column_names)             
corr_matrix=correlation_matrix(bootstrapped_table)
ind_matrix=independency_matrix(corr_matrix,args.threshold,args.outputname)





    
    
end = time.time()
print(end - start,'s')
    


