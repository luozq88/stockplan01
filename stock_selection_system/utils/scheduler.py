import schedule
import time
import logging
from typing import Callable, Dict
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import TASK_SCHEDULE


class TaskScheduler:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.tasks = {}
    
    def add_task(self, task_name: str, task_func: Callable, task_time: str):
        try:
            schedule.every().day.at(task_time).do(task_func)
            
            self.tasks[task_name] = {
                'func': task_func,
                'time': task_time,
                'enabled': True
            }
            
            self.logger.info(f"Added task: {task_name} at {task_time}")
            
        except Exception as e:
            self.logger.error(f"Error adding task {task_name}: {e}")
    
    def remove_task(self, task_name: str):
        try:
            if task_name in self.tasks:
                schedule.clear(task_name)
                del self.tasks[task_name]
                self.logger.info(f"Removed task: {task_name}")
            else:
                self.logger.warning(f"Task {task_name} not found")
                
        except Exception as e:
            self.logger.error(f"Error removing task {task_name}: {e}")
    
    def enable_task(self, task_name: str):
        if task_name in self.tasks:
            self.tasks[task_name]['enabled'] = True
            self.logger.info(f"Enabled task: {task_name}")
    
    def disable_task(self, task_name: str):
        if task_name in self.tasks:
            self.tasks[task_name]['enabled'] = False
            self.logger.info(f"Disabled task: {task_name}")
    
    def run_pending(self):
        schedule.run_pending()
    
    def start(self):
        self.logger.info("Starting task scheduler...")
        
        while True:
            try:
                self.run_pending()
                time.sleep(1)
            except KeyboardInterrupt:
                self.logger.info("Task scheduler stopped by user")
                break
            except Exception as e:
                self.logger.error(f"Error in task scheduler: {e}")
                time.sleep(5)
    
    def setup_from_config(self, task_functions: Dict[str, Callable]):
        for task_name, task_config in TASK_SCHEDULE.items():
            if task_config['enabled'] and task_name in task_functions:
                self.add_task(
                    task_name=task_name,
                    task_func=task_functions[task_name],
                    task_time=task_config['time']
                )
    
    def get_next_run_times(self) -> Dict:
        next_runs = {}
        
        for job in schedule.get_jobs():
            next_run = job.next_run
            next_runs[str(job)] = next_run.strftime('%Y-%m-%d %H:%M:%S') if next_run else None
        
        return next_runs
    
    def list_tasks(self) -> Dict:
        return self.tasks.copy()
