"""
获取yarn队列信息
"""
import pandas as pd
import requests
import time
import datetime
import os
from ...config.config import yarn_url



class pyyarn():
    """获取yarn队里信息"""

    def __init__(self, queue='ai'):
        self.queue = queue
        self.url = None
        self._init_params()

    def _init_params(self):
        """初始化一些默认参数"""
        # 如果yarn-url没有配置，就用默认的
        if not self.url:
            self.url = yarn_url

    def to_log(self, string, log=None):
        """打印"""
        log.info(string) if log else print(string)

    def get_yarn_info(self):
        """获取yarn的版本和状态"""
        # url='http://172.31.100.169:8088/ws/v1/cluster/info'
        url = self.url + '/ws/v1/cluster/info'
        r = requests.get(url)
        return r.json()['clusterInfo']

    def get_cluster_info(self):
        """获取集群资源信息"""
        # # 集群上一共提交了多少应用。          # 集群上一共完成了多少应用。
        # 集群上正在跑的应用有多少个。           # 多少个应用被killed掉了。
        # 多少个应用失败了。                    # 集群可供使用的内存有多少。
        # 集群已经使用的内存有多少。            # 集群总共的内存大小数。
        # 集群总共拥有的节点个数。              # 集群挂掉的节点个数。
        # 集群健在的节点个数。                  # 集群可以使用的VirtualCores个数。
        # 集群总共的VirtualCores个数。          # 集群已经分配了的VirtualCores个数。
        # 集群已经分配了的container个数。       # 集群上还剩下的container个数
        # 集群上正在pending状态的container个数。
        # url='http://172.31.100.169:8088/ws/v1/cluster/metrics'
        url = self.url + '/ws/v1/cluster/metrics'
        r = requests.get(url)
        info = r.json()['clusterMetrics']
        # 总资源：totalMB，可用资源：availableMB
        return info

    def get_cluster_nodes(self):
        """获取集群各个节点的资源信息和状态"""
        # url='http://172.31.100.169:8088/ws/v1/cluster/nodes'
        url = self.url + '/ws/v1/cluster/nodes'
        r = requests.get(url)
        nodes = r.json()['nodes']['node']
        return pd.DataFrame(nodes)

    def get_cluster_node(self, node_id):
        """某一具体节点信息"""
        # url = 'http://172.31.100.169:8088/ws/v1/cluster/nodes/master:45454'
        url = self.url + '/ws/v1/cluster/nodes/{nodeid}'.format(nodeid=node_id)
        r = requests.get(url)
        node = r.json()['node']
        return node  # 返回dict

    def get_queue_info(self):
        """获取所有队列资源信息，返回dataframe，数值表示百分比"""
        # 可以知道每个队列至多/至少可以拥有的资源数（包括内存和vCores数）
        # 可以知道每个队列最多可以运行的应用个数。
        # 可以知道每个队列使用的资源个数（memory和vcores）
        # 知道每个队列的调度策略。
        # url='http://172.31.100.169:8088/ws/v1/cluster/scheduler'
        url = self.url + '/ws/v1/cluster/scheduler'
        r = requests.get(url)
        info = r.json()['scheduler']['schedulerInfo']
        # 获取指定信息,数值表示百分比
        # queueName：队列名称
        # capacities：队列信息汇总，是一个字典
        # capacity：默认队列内存（百分比）
        # maxCapacity：超过默认队列内存时允许最大使用内存
        # usedCapacity：当前使用内存（相对于默认队列内存的比值）
        all_info = []
        # 先获取root队列的资源信息
        info_t = {}
        info_t['queue'] = info['queueName']
        info_t['capacity'] = info['capacities']['queueCapacitiesByPartition'][0]['capacity']
        info_t['maxCapacity'] = info['capacities']['queueCapacitiesByPartition'][0]['maxCapacity']
        info_t['usedCapacity'] = info['capacities']['queueCapacitiesByPartition'][0]['usedCapacity']
        all_info.append(info_t)
        # 循环获取子队列的数据
        for q in info['queues']['queue']:
            info_t = {}
            info_t['queue'] = q['queueName']
            info_t['capacity'] = q['capacity']
            info_t['maxCapacity'] = q['maxCapacity']
            info_t['usedCapacity'] = q['usedCapacity']
            all_info.append(info_t)
        # 转成dataframe
        all_info = pd.DataFrame(all_info)
        all_info.index = all_info['queue'].tolist()
        return all_info

    def get_applications(self, queue=None):
        """获取指定队列的正在运行的应用"""
        # url='http://172.31.100.169:8088/ws/v1/cluster/apps'
        url = self.url + '/ws/v1/cluster/apps'
        # 任务状态说明
        # YarnApplicationState = (
        #     ('ACCEPTED', 'Application has been accepted by the scheduler.'),
        #     ('FAILED', 'Application which failed.'),
        #     ('FINISHED', 'Application which finished successfully.'),
        #     ('KILLED', 'Application which was terminated by a user or admin.'),
        #     ('NEW', 'Application which was just created.'),
        #     ('NEW_SAVING', 'Application which is being saved.'),
        #     ('RUNNING', 'Application which is currently running.'),
        #     ('SUBMITTED', 'Application which has been submitted.'), )
        # FinalApplicationStatus = (
        #     ('FAILED', 'Application which failed.'),
        #     ('KILLED', 'Application which was terminated by a user or admin.'),
        #     ('SUCCEEDED', 'Application which finished successfully.'),
        #     ('UNDEFINED', 'Undefined state when either the application has not yet finished.') )
        #
        # legal_states = set([s for s, _ in YarnApplicationState])
        # legal_final_statuses = set([s for s, _ in FinalApplicationStatus])
        if not queue:
            queue = 'root'
        params = {'queue': queue}
        r = requests.get(url, params=params)    # 如果加了params参数，只返回当前队列正在运行的，如果不加参数，则返回全部队列的包括历史的任务
        # 如果队列为空，则需要造一个空的dataframe返回
        # 获取指定字段
        cols = ['id', 'name', 'queue', 'user', 'finalStatus', 'applicationType', 'progress', 'startedTime', 'state']
        if r.json()['apps'] is None:
            dic = dict([(key, '') for key in cols])
            return pd.DataFrame(pd.Series(dic)).T
        else:
            jobs = r.json()['apps']['app']
        # 转成dataframe
        jobs2 = []
        for job in jobs:
            job_t = {}
            for col in cols:
                job_t[col] = job[col]
            jobs2.append(job_t)
        # 转成dataframe
        application = pd.DataFrame(jobs2)
        return application

    def application_exists(self, queue='ai', job_name=None):
        """判断某个任务是否存在队列中"""
        all_jobs = self.get_applications(queue=queue)
        if job_name in all_jobs['name'].tolist():
            return True
        else:
            return False

    def kill_application(self, queue='ai', job_name=None):
        """杀死某个spark程序"""
        all_jobs = self.get_applications(queue=queue)
        for job_id in all_jobs.loc[all_jobs['name'] == job_name, 'id'].tolist():
            cmd = 'yarn application -kill ' + job_id
            os.system(cmd)

    # def kill_application(self, app_id):
    #     """杀死应用，发现暂时不可用，反正服务器内部异常"""
    #     # app_id='application_1521200127024_90777'
    #     # url="http://localhost:8088/ws/v1/cluster/apps/{appid}/state.format(appid=app_id)"
    #     url = self.url + '/ws/v1/cluster/apps/{appid}/state'.format(appid=app_id)
    #     headers = {"Content-Type": "application/json"}
    #     params = {"state": "KILLED"}
    #     r = requests.put(url, data=params, headers=headers)
    #     r.status_code

    def get_all_application_statistics(self):
        """统计所有应用信息，包括历史应用"""
        # 通过这个api可以统计出不同类型（mapreduce或spark）、不同状态（running或finished）的应用个数。
        url = self.url + '/ws/v1/cluster/appstatistics'
        r = requests.get(url)
        stat = r.json()['appStatInfo']['statItem']
        # count: 应用数目， state：状态类型，type：类型
        return pd.DataFrame(stat)

    def get_application_info(self, app_id):
        """获取某个app的具体信息"""
        # 启动应用的user。               # 应用名称。
        # 应用类型。                     # 应用的状态。
        # 应用所属队列。                 # 应用最终运行状态。
        # 应用开始/停止时间              # 应用的进度。
        # 应用耗时多久。
        # 分配给这个应用所在container的资源（memory和Vcores）。
        # 负责运行这个应用的container的个数。
        # app_id = 'application_1521200127024_81952'
        # url='http://172.31.100.169:8088/ws/v1/cluster/apps/'+app_id
        url = self.url + '/ws/v1/cluster/apps/{appid}'.format(appid=app_id)
        r = requests.get(url)
        # 重点关注：id，name, queue, allocatedMB，applicationType，progress,
        #  finalStatus，finishedTime，resourceRequests,
        #  runningContainers, startedTime, state, user 等字段
        return r.json()['app']  # 返回dict

    def get_application_attempts(self, app_id):
        """某个app_id的Attempts, 暂时不知道是什么意思"""
        # app_id = 'application_1521200127024_81952'
        # url='http://172.31.100.169:8088/ws/v1/cluster/apps/{app_id}/appattempts'.format(app_id=app_id)
        url = self.url + '/ws/v1/cluster/apps/{app_id}/appattempts'.format(appid=app_id)
        attempt = requests.get(url).json()['appAttempts']['appAttempt']
        return pd.DataFrame(attempt)

    def wait_for_yarn_memory(self, need, queue=None, starttime=None, endtime=None, waite=60, log=None):
        """
        等待yarn资源
        :param need:
        :param starttime: 判断时间区间开始
        :param endtime: 判断时间区间结束
        :param waite: 等待x秒后再去轮询
        :param log:
        :return:
        """
        queue = queue if queue else self.queue  # 如果没有指定队列，则用ai队列
        if (starttime == endtime) and (starttime or endtime):
            raise Exception('开始时间不应该等于结束时间')
        if not starttime:
            starttime = '00:00:00'
        if not endtime:
            endtime = '23:59:59'

        while True:
            cluster_info = self.get_cluster_info()  # 集群资源总资源：totalMB，集群可用资源：availableMB
            queue_info = self.get_queue_info()  # 获取所有队列资源信息
            queue_info = queue_info.loc[queue_info['queue'] == queue]  # 获取指定队列资源信息
            # queueName：队列名称
            # capacities：队列信息汇总，是一个字典
            # capacity：默认队列内存（百分比）
            # maxCapacity：超过默认队列内存时允许最大使用内存
            # usedCapacity：当前使用内存（相对于默认队列内存的比值）
            root_total = int(cluster_info['totalMB'] / 1000)  # 集群总资源
            root_free = int(cluster_info['availableMB'] / 1000)  # 集群剩余资源
            queue_max = root_total * queue_info.loc[queue_info['queue'] == queue, 'maxCapacity'].iloc[0] / 100  # 队列最大可用值
            queue_config = root_total * queue_info.loc[queue_info['queue'] == queue, 'capacity'].iloc[0] / 100  # 队列默认值
            queue_used = queue_config * queue_info.loc[queue_info['queue'] == queue, 'usedCapacity'].iloc[0] / 100  # 队列已使用值
            queue_free = queue_max - queue_used  # 队列空余可用值

            t = str(datetime.datetime.now())[:19]
            queue_info = '%s 当前 %s 队列的可用资源为 %dG, 集群的可用资源为 %dG,需求是 %dG,' \
                         % (t, queue, queue_free, root_free, need)
            # 如果集群资源或者子队列资源小于需求，则等待
            if queue_free > need and root_free > need:
                self.to_log(queue_info + '可以执行', log)
                return 1, ''
            else:
                self.to_log(queue_info + '等待%d秒' % waite, log)
            # 超时失败
            now = time.strftime("%H:%M:%S", time.localtime())  # '10:04:57'
            if starttime < endtime and not starttime <= now <= endtime:
                return 0, queue_info + '等待超时失败'
            # 处理跨日的超时
            if starttime > endtime and endtime <= now <= starttime:
                return 0, queue_info + '等待超时失败'
            # 等待中
            time.sleep(waite)


def _test():
    # 实例化
    yarn = pyyarn()
    # 获取集群资源信息
    yarn.get_cluster_info()
    # 获取所有队列正在使用资源情况
    yarn.get_queue_info()
    # 获取所有队里的正在运行的任务
    yarn.get_applications()
    # 获取指定队列的正在运行的任务
    yarn.get_applications(queue='ai')
    # 获取某个任务的情况
    b = yarn.get_applications(queue='bi')
    b.loc[b['name'] == 'a_t_newxx'].to_dict(orient='records')

    # 资源等待
    yarn.wait_for_yarn_memory(need=30000, queue='a',
                              starttime=None, endtime=None,
                              waite=60, log=None)
