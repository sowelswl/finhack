import os
import pandas as pd
from filelock import FileLock  # 需要安装：pip install filelock
import time
from runtime.constant import *
from finhack.library.config import Config
from finhack.factor.default.preCheck import preCheck
from finhack.factor.default.indicatorCompute import indicatorCompute
from finhack.factor.default.alphaEngine import alphaEngine
from finhack.market.astock.astock import AStock
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait, ALL_COMPLETED, as_completed
from finhack.factor.default.factorPkl import factorPkl
import finhack.library.log as Log
import psutil  # 添加psutil库用于监控内存

class taskRunner:
    @staticmethod
    def runTask(task_list, force_factors=None):
        """
        运行因子计算任务
        
        Parameters:
        -----------
        task_list : str
            要计算的任务列表，以逗号分隔
        force_factors : list, optional
            需要强制重新计算的因子列表
        """
        c_list = preCheck.checkAllFactors()  # chenged factor，代码发生变化

        # 如果没有提供强制刷新的因子列表，则创建空列表
        if force_factors is None:
            force_factors = []
            
        # 记录强制刷新的因子
        force_file = CACHE_DIR + '/force_factors.txt'
        if len(force_factors) > 0:
            with open(force_file, 'w', encoding='utf-8') as f:
                for factor in force_factors:
                    f.write(factor + '\n')
            Log.logger.info(f"将强制重新计算以下因子: {','.join(force_factors)}")
        elif os.path.exists(force_file):
            os.remove(force_file)

        os.system('rm -rf ' + CACHE_DIR + '/single_factors_tmp1/*')
        os.system('rm -rf ' + CACHE_DIR + '/single_factors_tmp2/*')
        
        # 需要刷新价格数据
        AStock.getStockDailyPrice(code_list=[], where="", startdate='', enddate='', fq='hfq', db='tushare', cache=False)
        
        # 遍历任务列表，收集所有要处理的indicator因子和alpha因子
        task_list = task_list.split(',')
        all_indicator_factors = []
        all_alpha_factors = {}
        
        # 收集所有indicator因子
        for factor_list_name in task_list:
            if os.path.exists(CONFIG_DIR + "/factorlist/indicatorlist/" + factor_list_name):
                with open(CONFIG_DIR + "/factorlist/indicatorlist/" + factor_list_name, 'r', encoding='utf-8') as f:
                    factor_list = [_.rstrip('\n') for _ in f.readlines()]
                    for i in range(len(factor_list)):
                        if not '_' in factor_list[i]:
                            factor_list[i] = factor_list[i] + '_0'
                    all_indicator_factors.extend(factor_list)
        
        # 找出所有因子中的最早最后更新日期，作为一致的计算窗口
        min_lastdate = taskRunner.get_min_factor_date(all_indicator_factors)
        if min_lastdate:
            Log.logger.info(f"使用最早因子日期 {min_lastdate} 作为计算基准")
        
        # 处理强制刷新的因子
        if force_factors:
            for factor in force_factors:
                factor_path = SINGLE_FACTORS_DIR + factor + '.csv'
                if os.path.exists(factor_path):
                    os.remove(factor_path)
                    Log.logger.info(f"已删除强制重新计算的因子文件: {factor}")
        
        # 计算indicator因子
        for factor_list_name in task_list:
            # factor列表
            if os.path.exists(CONFIG_DIR + "/factorlist/indicatorlist/" + factor_list_name):
                with open(CONFIG_DIR + "/factorlist/indicatorlist/" + factor_list_name, 'r', encoding='utf-8') as f:
                    factor_list = [_.rstrip('\n') for _ in f.readlines()]
                indicatorCompute.computeList(
                    list_name=factor_list_name,
                    factor_list=factor_list,
                    c_list=c_list,
                    min_lastdate=min_lastdate,
                    force_factors=force_factors
                )
        
        # 计算alpha因子
        for factor_list_name in task_list:
            if os.path.exists(CONFIG_DIR + "/factorlist/alphalist/" + factor_list_name):
                with open(CONFIG_DIR + "/factorlist/alphalist/" + factor_list_name, 'r', encoding='utf-8') as f:
                    factor_list = [_.rstrip('\n') for _ in f.readlines()]
                    i = 0
                    tasks = []
                    
                    # 动态调整进程数量
                    initial_workers = 1  # 初始进程数为1
                    max_workers = os.cpu_count() - 2 if os.cpu_count() > 2 else 1  # 最大进程数
                    current_workers = initial_workers
                    
                    # 记录初始内存使用情况
                    initial_memory = psutil.virtual_memory().available
                    memory_usage_history = []
                    
                    # 将因子列表分批处理
                    batch_size = 10  # 每批处理的因子数量
                    factor_batches = [factor_list[i:i+batch_size] for i in range(0, len(factor_list), batch_size)]
                    
                    Log.logger.info(f"Alpha因子计算开始，共 {len(factor_list)} 个因子，分为 {len(factor_batches)} 批处理")
                    
                    for batch_idx, batch_factors in enumerate(factor_batches):
                        Log.logger.info(f"处理第 {batch_idx+1}/{len(factor_batches)} 批Alpha因子，共 {len(batch_factors)} 个")
                        
                        # 根据当前内存使用情况调整工作进程数
                        if batch_idx > 0:  # 第一批使用初始进程数
                            time.sleep(5)  # 等待5秒，让内存使用情况稳定
                            # 计算每个进程的平均内存使用量
                            if memory_usage_history:
                                current_memory = psutil.virtual_memory().available
                                memory_used = initial_memory - current_memory
                                
                                # 记录内存使用情况
                                memory_usage_history.append(memory_used)
                                
                                # 如果平均每个进程内存使用量较低，可以增加进程数
                                if current_memory > 2 * 1024 * 1024 * 1024 and current_workers < max_workers:  # 有2GB以上可用内存
                                    new_workers = min(current_workers + 1, max_workers)
                                    Log.logger.info(f"内存充足，增加进程数: {current_workers} -> {new_workers}")
                                    current_workers = new_workers
                                
                                # 如果可用内存不足1GB，减少进程数
                                elif current_memory < 1 * 1024 * 1024 * 1024 and current_workers > 1:
                                    new_workers = max(1, current_workers - 1)
                                    Log.logger.info(f"内存不足，减少进程数: {current_workers} -> {new_workers}")
                                    current_workers = new_workers
                        
                        Log.logger.info(f"当前使用 {current_workers} 个工作进程")
                        
                        batch_tasks = []
                        with ProcessPoolExecutor(max_workers=current_workers) as pool:
                            for factor in batch_factors:   
                                i = i + 1
                                alpha_name = factor_list_name + '_' + str(i).zfill(3)
                                
                                # 检查是否需要强制重新计算
                                force_recalc = alpha_name in force_factors
                                
                                mytask = pool.submit(
                                    alphaEngine.calc,
                                    factor,
                                    pd.DataFrame(),
                                    alpha_name,
                                    False,  # check
                                    True,   # save
                                    False,  # ignore_notice
                                    [],     # stock_list
                                    not force_recalc  # diff - 如果强制重新计算，则不做差异检查
                                )
                                batch_tasks.append(mytask)
                            
                            # 等待当前批次的任务完成
                            for future in as_completed(batch_tasks):
                                try:
                                    result = future.result()
                                except Exception as e:
                                    Log.logger.error(f"Alpha因子计算出错: {str(e)}")
                            
                            tasks.extend(batch_tasks)
                        
                        # 每批次处理完后，强制进行垃圾回收
                        import gc
                        gc.collect()
                        time.sleep(2)  # 等待5秒，让内存使用情况稳定
                    
                    # 等待所有alpha因子计算完成
                    wait(tasks, return_when=ALL_COMPLETED)
        
        os.system('mv ' + CACHE_DIR + '/single_factors_tmp2/* ' + SINGLE_FACTORS_DIR)
        factorPkl.save()
    
    @staticmethod
    def get_min_factor_date(factor_list):
        """获取因子列表中的最早最后更新日期"""
        min_lastdate = None
        today = time.strftime("%Y%m%d", time.localtime())
        
        for factor_name in factor_list:
            single_factors_path = SINGLE_FACTORS_DIR + factor_name + '.csv'
            if os.path.isfile(single_factors_path):
                try:
                    # 尝试只读取文件的最后几行来提高效率
                    df = pd.read_csv(single_factors_path, header=None, 
                                 names=['ts_code', 'trade_date', factor_name],
                                 nrows=1000)  # 只读取最后1000行
                    factor_lastdate = str(df['trade_date'].max())
                    
                    if min_lastdate is None or factor_lastdate < min_lastdate:
                        min_lastdate = factor_lastdate
                except Exception as e:
                    Log.logger.error(f"读取因子 {factor_name} 日期时出错: {str(e)}")
        
        return min_lastdate