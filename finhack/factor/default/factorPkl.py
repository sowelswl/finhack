import os
import pandas as pd
import multiprocessing
import shutil
from runtime.constant import *
import dask.dataframe as dd
from finhack.market.astock.astock import AStock
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import psutil  # 添加psutil库用于监控内存
import time  # 添加time库用于暂停

class factorPkl:
    def process_file(file_path, output_dir, all_dates_df):
        # 读取 CSV 文件
        factor_name = os.path.basename(file_path).split('.')[0]
        df = pd.read_csv(file_path, names=['ts_code', 'trade_date', factor_name])
        # 设置索引
        df.set_index(['ts_code', 'trade_date'], inplace=True)
        # 对齐索引并填充缺失数据
        aligned_df = all_dates_df.join(df, how='left')
        aligned_df[factor_name] = aligned_df[factor_name].fillna(method='ffill')
        
        # 按照索引排序
        aligned_df.sort_index(inplace=True)
        
        # 重置索引之前，按照 ts_code 和 trade_date 排序
        aligned_df = aligned_df.reset_index().sort_values(by=['ts_code', 'trade_date'])
        
        # 仅保存因子值到 pkl 文件
        aligned_df[[factor_name]].to_pickle(os.path.join(output_dir, f'{factor_name}.pkl'))
        
        
    def save():
        # 设置 CSV 文件所在的目录
        directory = SINGLE_FACTORS_DIR
        output_dir = SINGLE_FACTORS_PKL_TMP_DIR
        open_path = SINGLE_FACTORS_DIR+'open.csv'
        open_df = pd.read_csv(open_path, names=['ts_code', 'trade_date', 'open'])
        open_df = open_df.sort_values(by=['ts_code', 'trade_date'])
        # 设置 'ts_code' 和 'trade_date' 为索引
        open_df.set_index(['ts_code', 'trade_date'], inplace=True)
        
        # 创建一个基于这些索引的 DataFrame
        all_dates_df = pd.DataFrame(index=open_df.index)
        
        # 检查 output_dir是否存在
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)

        os.makedirs(output_dir)        
        
        # 获取所有需要处理的文件
        files_to_process = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]
        
        # 动态调整进程数量
        max_processes = os.cpu_count()  # 最大进程数为CPU核心数
        current_processes = 1  # 初始进程数为1
        
        # 创建进程池，初始只有1个进程
        with ProcessPoolExecutor(max_workers=current_processes) as executor:
            # 记录初始内存使用情况
            initial_memory = psutil.virtual_memory().available
            
            # 创建任务队列
            futures = {}
            pending_files = files_to_process.copy()
            
            # 提交第一个任务
            if pending_files:
                file_path = pending_files.pop(0)
                futures[executor.submit(factorPkl.process_file, file_path, output_dir, all_dates_df)] = file_path
            
            # 处理所有文件
            while futures or pending_files:
                # 等待任意一个任务完成
                done, _ = as_completed(futures.keys(), timeout=1), None
                
                # 处理完成的任务
                for future in done:
                    file_path = futures.pop(future)
                    try:
                        future.result()  # 获取结果，捕获可能的异常
                        print(f"处理完成: {os.path.basename(file_path)}")
                    except Exception as e:
                        print(f"处理失败: {os.path.basename(file_path)}, 错误: {e}")
                
                # 检查当前内存使用情况
                current_memory = psutil.virtual_memory().available
                memory_usage_per_process = (initial_memory - current_memory) / max(1, len(futures))
                
                # 判断是否可以增加进程数
                if (current_processes < max_processes and 
                    current_memory > 2 * 1024 * 1024 * 1024 and  # 确保有至少2GB可用内存
                    memory_usage_per_process * (current_processes + 1) < current_memory * 0.5):  # 确保增加进程后内存使用不超过可用内存的50%
                    current_processes += 1
                    print(f"增加进程数到 {current_processes}")
                    
                    # 重新创建进程池
                    new_executor = ProcessPoolExecutor(max_workers=current_processes)
                    
                    # 将未完成的任务转移到新的进程池
                    new_futures = {}
                    for future, path in futures.items():
                        if not future.done():
                            new_futures[new_executor.submit(factorPkl.process_file, path, output_dir, all_dates_df)] = path
                    
                    # 关闭旧的进程池
                    executor.shutdown(wait=False)
                    
                    # 更新进程池和任务列表
                    executor = new_executor
                    futures = new_futures
                
                # 判断是否需要减少进程数
                elif current_memory < 1 * 1024 * 1024 * 1024 and current_processes > 1:  # 可用内存小于1GB
                    current_processes = max(1, current_processes - 1)
                    print(f"减少进程数到 {current_processes}")
                    
                    # 重新创建进程池
                    new_executor = ProcessPoolExecutor(max_workers=current_processes)
                    
                    # 将未完成的任务转移到新的进程池
                    new_futures = {}
                    for future, path in futures.items():
                        if not future.done():
                            new_futures[new_executor.submit(factorPkl.process_file, path, output_dir, all_dates_df)] = path
                    
                    # 关闭旧的进程池
                    executor.shutdown(wait=False)
                    
                    # 更新进程池和任务列表
                    executor = new_executor
                    futures = new_futures
                
                # 提交新任务，直到达到当前进程数
                while len(futures) < current_processes and pending_files:
                    file_path = pending_files.pop(0)
                    futures[executor.submit(factorPkl.process_file, file_path, output_dir, all_dates_df)] = file_path
                
                # 如果没有正在处理的任务但还有待处理文件，提交一个新任务
                if not futures and pending_files:
                    file_path = pending_files.pop(0)
                    futures[executor.submit(factorPkl.process_file, file_path, output_dir, all_dates_df)] = file_path
                
                # 短暂休息，避免CPU过度使用
                time.sleep(0.1)
        
        # 单独保存全量索引
        index_df = all_dates_df.reset_index()
        index_df['trade_date']=index_df['trade_date'].astype(str)
        index_df.to_pickle(output_dir+'index.pkl')
        
        if os.path.exists(SINGLE_FACTORS_PKL_OLD_DIR):
            shutil.rmtree(SINGLE_FACTORS_PKL_OLD_DIR)
        
        os.rename(SINGLE_FACTORS_PKL_DIR, SINGLE_FACTORS_PKL_OLD_DIR)
        os.rename(SINGLE_FACTORS_PKL_TMP_DIR, SINGLE_FACTORS_PKL_DIR)
        shutil.rmtree(SINGLE_FACTORS_PKL_OLD_DIR)