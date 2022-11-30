import os
from library.config import config
from factors.preCheck import preCheck
from factors.indicatorCompute import indicatorCompute
from factors.alphaEngine import alphaEngine
from library.astock import AStock
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, ALL_COMPLETED
import pandas as pd

class taskRunner:
    def runTask(taskName='all'):
        c_list=preCheck.checkAllFactors()
        if taskName=='all':
            task_list=config.getSectionList('task')
        else:
            task_list=[taskName]

        mypath=os.path.dirname(os.path.dirname(__file__))
        
        os.system('rm -rf '+mypath+'/data/single_factors_tmp1/*')
        #os.system('rm -rf '+mypath+'/data/code_factors_tmp')
        os.system('rm -rf '+mypath+'/data/single_factors_tmp2/*')
        # os.system('mkdir '+mypath+'/data/single_factors_tmp1')
        # os.system('mkdir '+mypath+'/data/single_factors_tmp2')
        # os.system('mkdir '+mypath+'/data/code_factors_tmp')
        
        #遍历任务列表
        for task in task_list:
            task=config.getConfig('task',task)
            factor_lists=task['list'].split(',')
            for factor_list_name in factor_lists:
                    #factor列表
                if os.path.exists(mypath+"/lists/factorlist/"+factor_list_name):
                    with open(mypath+"/lists/factorlist/"+factor_list_name, 'r', encoding='utf-8') as f:
                        factor_list=[_.rstrip('\n') for _ in f.readlines()]
                    indicatorCompute.computeList(list_name=factor_list_name,factor_list=factor_list,c_list=c_list)
            os.system('mv '+mypath+'/data/single_factors_tmp2/* '+mypath+'/data/single_factors/')
         
            #alpha列表
            for factor_list_name in factor_lists:
                if os.path.exists(mypath+"/lists/alphalist/"+factor_list_name):
                    with open(mypath+"/lists/alphalist/"+factor_list_name, 'r', encoding='utf-8') as f:
                        factor_list=[_.rstrip('\n') for _ in f.readlines()]
                        i=0
                        
                        # for factor in factor_list: 
                        #     i=i+1
                        #     alpha_name=factor_list_name+'_'+str(i).zfill(3)                            
                        #     df=alphaEngine.calc(factor,pd.DataFrame(),alpha_name)
                        #     print(df)
                        #     exit()
                        
                        with ProcessPoolExecutor(max_workers=8) as pool:
                            for factor in factor_list:   
                                i=i+1
                                alpha_name=factor_list_name+'_'+str(i).zfill(3)
                                mytask=pool.submit(alphaEngine.calc,factor,pd.DataFrame(),alpha_name)
                                
        
            
        pass
    
    
    
    
    