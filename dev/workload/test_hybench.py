import threading
import random
import string
import sys
import os
import time
from datetime import datetime
import re

sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev"))

import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from config import Config

try:
    import tomllib  # Python 3.11+
except ImportError:
    try:
        import tomli as tomllib  # Python 3.7-3.10
    except ImportError:
        # Fallback to manual parsing if tomllib/tomli not available
        tomllib = None


def get_connection(autocommit: bool = True) -> MySQLConnection:
    """获取数据库连接
    
    可以通过环境变量或 Config 类配置数据库连接信息
    环境变量优先级: TIDB_HOST, TIDB_PORT, TIDB_USER, TIDB_PASSWORD, TIDB_DB
    """
    config = Config()
    
    # 优先使用环境变量，否则使用硬编码值或 Config 类
    db_conf = {
        "host": config.TIDB_HOST,
        "port": config.TIDB_PORT,
        "user": config.TIDB_USER,
        "password": config.TIDB_PASSWORD,
        "database": "hybench", #config.TIDB_DB_NAME,
        "autocommit": autocommit,
        # mysql-connector-python will use C extension by default,
        # to make this example work on all platforms more easily,
        # we choose to use pure python implementation.
        "use_pure": True
    }

    if hasattr(config, 'ca_path') and config.ca_path:
        db_conf["ssl_verify_cert"] = True
        db_conf["ssl_verify_identity"] = True
        db_conf["ssl_ca"] = config.ca_path
    return mysql.connector.connect(**db_conf)


class HyBench_Parameter:
    def __init__(self):
        # 参数范围，需要根据实际数据调整
        self.max_custid = 300000
        self.max_companyid = 302000
        self.max_accountid = 302000
        self.max_sourceid = 301999
        self.max_targetid = 301999
        self.max_applicantid = 301999
        
        # 公司类别列表
        self.company_categories = [
            'software_IT', 'internet_service', 'telecommunication',
            'technology_service', 'computer_communication_manufacturing'
        ]
        
        # 事务类型
        self.transfer_types = ['salary', 'invest', 'transfer']
        self.checking_types = ['checking', 'transfer']
        
        # SQL 文件路径
        self.sql_file_path = '/data3/dzh/project/HyBench-2024/conf/stmt_mysql.toml'


class HyBench_Statistics:
    def __init__(self):
        # AP 查询统计 (AP-1 到 AP-13)
        self.ap_query_cnt = [0] * 13
        self.ap_query_lat = [0.0] * 13
        self.ap_query_lat_sum = [0.0] * 13
        
        # TP 查询统计 (TP-1 到 TP-18)
        self.tp_query_cnt = [0] * 18
        self.tp_query_lat = [0.0] * 18
        self.tp_query_lat_sum = [0.0] * 18
        
        # AT 查询统计 (AT-00, AT-0, AT-1 到 AT-6, AT-3.1, AT-4.1, AT-5.1, AT-6.1)
        self.at_query_cnt = {}
        self.at_query_lat = {}
        self.at_query_lat_sum = {}
        
        # IQ 查询统计 (IQ-1 到 IQ-6, IQ-5.1)
        self.iq_query_cnt = {}
        self.iq_query_lat = {}
        self.iq_query_lat_sum = {}
        
        # Fresh 查询统计
        self.fresh_query_cnt = {}
        self.fresh_query_lat = {}
        self.fresh_query_lat_sum = {}


hybench_stats = HyBench_Statistics()


def parse_toml_file(file_path):
    """解析 TOML 文件，返回 SQL 语句字典"""
    sqls = {}
    
    try:
        # 尝试使用 tomllib (Python 3.11+)
        if tomllib:
            with open(file_path, 'rb') as f:
                data = tomllib.load(f)
                for key, value in data.items():
                    sqls[key] = value
            return sqls
    except:
        pass
    
    # 手动解析 TOML 文件
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 使用正则表达式解析
    # 匹配 [section] 和 sql = ... 的模式
    pattern = r'\[([^\]]+)\]\s*\n\s*sql\s*=\s*(.*?)(?=\n\[|\Z)'
    matches = re.finditer(pattern, content, re.DOTALL | re.MULTILINE)
    
    for match in matches:
        key = match.group(1).strip()
        sql_value = match.group(2).strip()
        
        # 处理多行字符串 """..."""
        if sql_value.startswith('"""'):
            # 找到结束的 """
            end_idx = sql_value.find('"""', 3)
            if end_idx != -1:
                sql_value = sql_value[3:end_idx].strip()
            else:
                sql_value = sql_value[3:].strip()
        # 处理单行字符串 "..."
        elif sql_value.startswith('"') and sql_value.endswith('"'):
            sql_value = sql_value[1:-1].strip()
        # 处理数组格式 [...]
        elif sql_value.startswith('['):
            # 解析数组中的 SQL 语句
            lines = []
            in_string = False
            current_line = ""
            i = 1  # 跳过开始的 [
            while i < len(sql_value):
                char = sql_value[i]
                if char == '"' and (i == 0 or sql_value[i-1] != '\\'):
                    in_string = not in_string
                    current_line += char
                elif char == ']' and not in_string:
                    if current_line.strip():
                        # 移除引号和逗号
                        line = current_line.strip().strip(',').strip('"').strip()
                        if line:
                            lines.append(line)
                    break
                elif char == ',' and not in_string:
                    if current_line.strip():
                        line = current_line.strip().strip(',').strip('"').strip()
                        if line:
                            lines.append(line)
                        current_line = ""
                    else:
                        current_line += char
                else:
                    current_line += char
                i += 1
            if current_line.strip():
                line = current_line.strip().strip(',').strip('"').strip()
                if line:
                    lines.append(line)
            sql_value = lines if lines else sql_value
        
        sqls[key] = sql_value
    
    return sqls


def count_parameters(sql):
    """统计 SQL 中 ? 占位符的数量"""
    if isinstance(sql, list):
        return sum(sql.count('?') for sql in sql)
    return sql.count('?')


def generate_random_params(param_count, query_type, query_name, wl_param):
    """根据查询类型生成随机参数"""
    params = []
    
    # 对于没有参数的查询，直接返回空列表
    if param_count == 0:
        return params
    
    for i in range(param_count):
        if query_type == 'AP':
            if query_name == 'AP-1':
                # sourceid, targetid (重复4次)
                if i < 2:
                    val = random.randint(1, wl_param.max_sourceid)
                else:
                    val = params[i % 2]  # 重复使用前两个值
                params.append(val)
            elif query_name == 'AP-2':
                params.append(random.randint(1, wl_param.max_custid))
            elif query_name == 'AP-2.1':
                params.append(random.randint(1, wl_param.max_applicantid))
            elif query_name in ['AP-3', 'AP-4']:
                params.append(random.randint(1, wl_param.max_custid))
            elif query_name == 'AP-5':
                params.append(random.randint(1, wl_param.max_companyid))
            elif query_name == 'AP-7':
                params.append(random.randint(1, wl_param.max_companyid))
            elif query_name == 'AP-12':
                params.append(random.choice(wl_param.company_categories))
            elif query_name == 'AP-13':
                params.append(random.randint(1, wl_param.max_sourceid))
            else:
                params.append(random.randint(1, 100000))
                
        elif query_type == 'TP':
            if query_name == 'TP-1':
                params.append(random.randint(1, wl_param.max_custid))
            elif query_name == 'TP-2':
                params.append(random.randint(1, wl_param.max_companyid))
            elif query_name in ['TP-3', 'TP-4']:
                params.append(random.randint(1, wl_param.max_accountid))
            elif query_name in ['TP-5', 'TP-6']:
                # TP-5 和 TP-6 需要两个参数：sourceid 和 targetid
                params.append(random.randint(1, wl_param.max_custid))
            elif query_name in ['TP-7', 'TP-8']:
                params.append(random.randint(1, wl_param.max_applicantid))
            elif query_name == 'TP-9':
                # 6个参数: accountid, amount, accountid, amount, accountid, sourceid, targetid, amount, type, timestamp
                if i == 0 or i == 2 or i == 4:
                    params.append(random.randint(1, wl_param.max_accountid))
                elif i == 1 or i == 3 or i == 7:
                    params.append(random.uniform(1.0, 10000.0))
                elif i == 5:
                    params.append(random.randint(1, wl_param.max_sourceid))
                elif i == 6:
                    params.append(random.randint(1, wl_param.max_targetid))
                elif i == 8:
                    params.append(random.choice(wl_param.transfer_types))
                elif i == 9:
                    params.append(datetime.now())
            elif query_name == 'TP-10':
                # 类似 TP-9
                if i == 0:
                    params.append(random.randint(1, wl_param.max_companyid))
                elif i == 1 or i == 3 or i == 6:
                    params.append(random.uniform(1.0, 10000.0))
                elif i == 2 or i == 4:
                    params.append(random.randint(1, wl_param.max_accountid))
                elif i == 5:
                    params.append(random.randint(1, wl_param.max_sourceid))
                elif i == 7:
                    params.append(random.randint(1, wl_param.max_targetid))
                elif i == 8:
                    params.append(random.choice(wl_param.transfer_types))
                elif i == 9:
                    params.append(datetime.now())
            elif query_name == 'TP-11':
                # 类似 TP-9
                if i == 0 or i == 2 or i == 4:
                    params.append(random.randint(1, wl_param.max_accountid))
                elif i == 1 or i == 3 or i == 6:
                    params.append(random.uniform(1.0, 10000.0))
                elif i == 5:
                    params.append(random.randint(1, wl_param.max_sourceid))
                elif i == 7:
                    params.append(random.randint(1, wl_param.max_targetid))
                elif i == 8:
                    params.append(random.choice(wl_param.checking_types))
                elif i == 9:
                    params.append(datetime.now())
            elif query_name == 'TP-12':
                # TP-12: 11个参数，按顺序生成
                # 参数顺序: custID(1), amount(2), custID(3), companyID(4), amount(5), companyID(6),
                #          applicantid(7), amount(8), duration(9), timestamp(10), timestamp(11)
                if i == 0 or i == 2:
                    params.append(random.randint(1, wl_param.max_custid))
                elif i == 1 or i == 4 or i == 7:
                    params.append(random.uniform(1.0, 100000.0))
                elif i == 3 or i == 5:
                    params.append(random.randint(1, wl_param.max_companyid))
                elif i == 6:
                    params.append(random.randint(1, wl_param.max_applicantid))
                elif i == 8:
                    params.append(random.randint(1, 365))
                elif i == 9 or i == 10:
                    params.append(datetime.now())
            elif query_name in ['TP-13', 'TP-14', 'TP-15', 'TP-16']:
                # 这些查询的第一条 SQL 没有参数，参数从查询结果获取
                # 所以这里不生成参数，参数会在执行时从查询结果获取
                pass  # 不生成参数
            elif query_name in ['TP-17', 'TP-18']:
                if i == 0 or i == 1 or i == 3 or i == 5:
                    params.append(random.randint(1, wl_param.max_accountid))
                elif i == 2 or i == 4:
                    params.append(random.uniform(1.0, 10000.0))
            else:
                params.append(random.randint(1, 100000))
                
        elif query_type == 'AT':
            if query_name in ['AT-00', 'AT-0']:
                params.append(random.randint(1, wl_param.max_companyid))
            elif query_name in ['AT-1', 'AT-2']:
                if i == 0 or i == 2 or i == 4:
                    params.append(random.randint(1, wl_param.max_accountid))
                elif i == 1:
                    params.append(random.randint(1, wl_param.max_sourceid))
                elif i == 3 or i == 5:
                    params.append(random.uniform(1.0, 10000.0))
                elif i == 6:
                    params.append(random.randint(1, wl_param.max_sourceid))
                elif i == 7:
                    params.append(random.randint(1, wl_param.max_targetid))
                elif i == 8:
                    params.append(random.choice(wl_param.transfer_types if query_name == 'AT-1' else wl_param.checking_types))
                elif i == 9:
                    params.append(datetime.now())
            elif query_name == 'AT-3':
                if i == 0:
                    params.append(random.randint(1, wl_param.max_accountid))
                elif i == 1 or i == 2:
                    params.append(random.randint(1, wl_param.max_accountid))
                # 其他参数需要从查询结果获取
            elif query_name == 'AT-4':
                if i == 0:
                    params.append(random.randint(1, wl_param.max_accountid))
                elif i == 1:
                    params.append(random.uniform(1.0, 100000.0))
                elif i == 2:
                    params.append(datetime.now())
            elif query_name == 'AT-5':
                params.append(random.randint(1, wl_param.max_applicantid))
            elif query_name == 'AT-6':
                params.append(random.randint(1, wl_param.max_applicantid))
            else:
                params.append(random.randint(1, 100000))
                
        elif query_type == 'IQ':
            if query_name == 'IQ-1':
                # IQ-1 需要两个参数：sourceid 和 targetid
                params.append(random.randint(1, wl_param.max_sourceid))  # sourceid
                if param_count > 1:
                    params.append(random.randint(1, wl_param.max_targetid))  # targetid
            elif query_name == 'IQ-2':
                # IQ-2 需要两个参数：sourceid 和 targetid
                params.append(random.randint(1, wl_param.max_sourceid))  # sourceid
                if param_count > 1:
                    params.append(random.randint(1, wl_param.max_sourceid))  # targetid (实际也是 sourceid)
            elif query_name == 'IQ-3':
                params.append(random.randint(1, wl_param.max_companyid))
            elif query_name == 'IQ-4':
                # IQ-4 需要两个参数：两个 sourceid
                params.append(random.randint(1, wl_param.max_sourceid))  # 第一个 sourceid
                if param_count > 1:
                    params.append(random.randint(1, wl_param.max_sourceid))  # 第二个 sourceid
            elif query_name == 'IQ-5':
                # IQ-5 需要 1 个参数：sourceid
                params.append(random.randint(1, wl_param.max_sourceid))
            elif query_name == 'IQ-5.1':
                # IQ-5.1 没有参数
                pass
            elif query_name == 'IQ-6':
                # IQ-6 没有参数
                pass
            else:
                params.append(random.randint(1, 100000))
                
        elif query_type == 'fresh':
            if query_name == 'fresh':
                params.append(random.randint(1, wl_param.max_targetid))
            elif query_name == 'fresh-1':
                params.append(random.randint(1, wl_param.max_companyid))
            else:
                params.append(random.randint(1, 100000))
        else:
            # 默认随机生成
            params.append(random.randint(1, 100000))
    
    return params


def execute_sql_with_params(cursor, sql, params, query_name, query_type):
    """执行带参数的 SQL"""
    try:
        if isinstance(sql, list):
            # 事务类型，需要执行多条 SQL
            results = []
            param_idx = 0  # 当前参数索引
            
            for i, stmt in enumerate(sql):
                stmt_params = []
                param_count = stmt.count('?')
                
                # 对于某些查询，需要从前面的结果获取参数
                # TP-13, TP-14, TP-15, TP-16 的第一条 SQL 没有参数
                if query_name in ['TP-13', 'TP-14', 'TP-15', 'TP-16'] and i == 0:
                    # 第一条 SQL 没有参数，直接执行
                    stmt_params = []
                elif query_name == 'TP-13' and i > 0:
                    if i == 1:  # INSERT INTO LOANTRANS
                        # 从第一个查询的结果获取参数
                        if results and len(results[0]) > 0:
                            row = results[0][0]
                            stmt_params = [
                                row[0] if len(row) > 0 else random.randint(1, 100000),  # ID
                                row[1] if len(row) > 1 else random.randint(1, 100000),  # APPLICANTID
                                row[2] if len(row) > 2 else random.uniform(1.0, 100000.0),  # AMOUNT
                                row[3] if len(row) > 3 else random.randint(1, 365),  # DURATION
                                datetime.now(),  # contract_timestamp
                                'accept',  # status
                                datetime.now(),  # loantrans_ts
                                0,  # delinquency
                            ]
                        else:
                            stmt_params = [random.randint(1, 100000)] * param_count
                    elif i == 2:  # UPDATE CUSTOMER
                        if results and len(results[0]) > 0:
                            row = results[0][0]
                            stmt_params = [
                                row[2] if len(row) > 2 else random.uniform(1.0, 100000.0),  # amount
                                row[1] if len(row) > 1 else random.randint(1, 100000),  # custID
                            ]
                        else:
                            stmt_params = [random.uniform(1.0, 100000.0), random.randint(1, 100000)]
                    elif i == 3:  # UPDATE COMPANY
                        if results and len(results[0]) > 0:
                            row = results[0][0]
                            stmt_params = [
                                row[2] if len(row) > 2 else random.uniform(1.0, 100000.0),  # amount
                                row[1] if len(row) > 1 else random.randint(1, 100000),  # companyID
                            ]
                        else:
                            stmt_params = [random.uniform(1.0, 100000.0), random.randint(1, 100000)]
                    elif i == 4:  # UPDATE LOANAPPS
                        if results and len(results[0]) > 0:
                            row = results[0][0]
                            stmt_params = [
                                'accept' if random.random() > 0.5 else 'reject',  # status
                                row[0] if len(row) > 0 else random.randint(1, 100000),  # ID
                            ]
                        else:
                            stmt_params = ['accept', random.randint(1, 100000)]
                elif query_name == 'TP-14' and i > 0:
                    if i == 1:  # UPDATE SAVINGACCOUNT
                        if results and len(results[0]) > 0:
                            row = results[0][0]
                            stmt_params = [
                                row[2] if len(row) > 2 else random.uniform(1.0, 100000.0),  # amount
                                row[1] if len(row) > 1 else random.randint(1, 100000),  # accountID
                            ]
                        else:
                            stmt_params = [random.uniform(1.0, 100000.0), random.randint(1, 100000)]
                    elif i == 2:  # UPDATE LOANTRANS
                        if results and len(results[0]) > 0:
                            row = results[0][0]
                            stmt_params = [
                                datetime.now(),  # loantrans_ts
                                row[0] if len(row) > 0 else random.randint(1, 100000),  # id
                            ]
                        else:
                            stmt_params = [datetime.now(), random.randint(1, 100000)]
                elif query_name == 'TP-15' and i > 0:
                    if results and len(results[0]) > 0:
                        row = results[0][0]
                        stmt_params = [
                            datetime.now(),  # loantrans_ts
                            row[0] if len(row) > 0 else random.randint(1, 100000),  # id
                        ]
                    else:
                        stmt_params = [datetime.now(), random.randint(1, 100000)]
                elif query_name == 'TP-16' and i > 0:
                    if i == 1:  # SELECT balance
                        # 从第一个查询获取 accountid
                        if results and len(results[0]) > 0:
                            row = results[0][0]
                            accountid = row[1] if len(row) > 1 else random.randint(1, 100000)
                            stmt_params = [accountid]
                        else:
                            stmt_params = [random.randint(1, 100000)]
                    elif i == 2:  # UPDATE SAVINGACCOUNT
                        if results and len(results[0]) > 0:
                            row = results[0][0]
                            amount = row[3] if len(row) > 3 else random.uniform(1.0, 10000.0)
                            accountid = results[1][0][0] if len(results) > 1 and len(results[1]) > 0 else random.randint(1, 100000)
                            stmt_params = [amount, accountid]
                        else:
                            stmt_params = [random.uniform(1.0, 10000.0), random.randint(1, 100000)]
                    elif i == 3:  # UPDATE LOANTRANS
                        if results and len(results[0]) > 0:
                            row = results[0][0]
                            stmt_params = [
                                datetime.now(),  # loantrans_ts
                                row[0] if len(row) > 0 else random.randint(1, 100000),  # id
                            ]
                        else:
                            stmt_params = [datetime.now(), random.randint(1, 100000)]
                elif query_name == 'TP-17' or query_name == 'TP-18':
                    # TP-17 和 TP-18: 每条 SQL 都有参数，从 params 中按顺序取
                    if param_count > 0:
                        if param_idx + param_count <= len(params):
                            stmt_params = params[param_idx:param_idx + param_count]
                            param_idx += param_count
                        else:
                            # 如果参数不够，生成随机参数
                            wl_param = HyBench_Parameter()
                            if i == 0 or i == 1:
                                stmt_params = [random.randint(1, wl_param.max_accountid)]
                            else:
                                stmt_params = [random.uniform(1.0, 10000.0), random.randint(1, wl_param.max_accountid)]
                elif query_name == 'AT-00' or query_name == 'AT-0':
                    if i == 0:
                        # 第一条 SQL 需要参数
                        if param_idx < len(params):
                            stmt_params = [params[param_idx]]
                            param_idx += 1
                    elif i == 1:
                        # 第二条 SQL 需要参数，应该从第一条的结果获取，但这里简化处理
                        if param_idx < len(params):
                            stmt_params = [params[param_idx]]
                            param_idx += 1
                elif query_name == 'AT-3' and i > 1:
                    # 需要从前面查询获取参数
                    if i == 2:  # INSERT INTO LOANTRANS
                        # 简化处理，使用随机值
                        stmt_params = [
                            random.randint(1, 100000),  # appid
                            random.randint(1, 100000),  # applicantid
                            random.uniform(1.0, 100000.0),  # amount
                            random.randint(1, 365),  # duration
                            datetime.now(),  # contract_timestamp
                            'accept',  # status
                            datetime.now(),  # loantrans_ts
                            0,  # delinquency
                        ]
                    elif i == 3:  # UPDATE LoanApps accept
                        stmt_params = [datetime.now(), random.randint(1, 100000)]
                    elif i == 4:  # UPDATE LoanApps reject
                        stmt_params = [datetime.now(), random.randint(1, 100000)]
                elif query_name == 'AT-4' and i > 0:
                    if i == 1:  # UPDATE savingAccount
                        stmt_params = [random.uniform(1.0, 100000.0), random.randint(1, 100000)]
                    elif i == 2:  # UPDATE LoanTrans
                        stmt_params = [datetime.now(), random.randint(1, 100000)]
                elif query_name == 'AT-5' and i > 0:
                    if results and len(results[0]) > 0:
                        row = results[0][0]
                        stmt_params = [row[0] if len(row) > 0 else random.randint(1, 100000)]
                    else:
                        stmt_params = [random.randint(1, 100000)]
                elif query_name == 'AT-6' and i > 0:
                    stmt_params = [random.randint(1, 100000)]
                else:
                    # 普通情况，从 params 中按顺序取参数
                    if param_count > 0:
                        if param_idx + param_count <= len(params):
                            stmt_params = params[param_idx:param_idx + param_count]
                            param_idx += param_count
                        else:
                            # 如果参数不够，生成随机参数
                            wl_param = HyBench_Parameter()
                            stmt_params = generate_random_params(param_count, query_type, query_name, wl_param)
                
                # 执行 SQL
                if param_count > 0:
                    if len(stmt_params) == param_count:
                        cursor.execute(stmt, tuple(stmt_params))
                    elif len(stmt_params) < param_count:
                        # 参数不够，生成缺失的参数
                        wl_param = HyBench_Parameter()
                        missing_count = param_count - len(stmt_params)
                        missing_params = generate_random_params(missing_count, query_type, query_name, wl_param)
                        stmt_params.extend(missing_params)
                        cursor.execute(stmt, tuple(stmt_params))
                    else:
                        # 参数太多，只使用前 param_count 个
                        cursor.execute(stmt, tuple(stmt_params[:param_count]))
                else:
                    # 没有参数，直接执行
                    cursor.execute(stmt)
                
                # 确保所有查询结果都被读取
                if stmt.strip().upper().startswith('SELECT'):
                    try:
                        result = cursor.fetchall()
                        results.append(result)
                    except Exception as e:
                        # 如果没有结果，返回空列表
                        results.append([])
                else:
                    results.append(None)
            
            return results
        else:
            # 单条 SQL
            param_count = sql.count('?')
            if param_count > 0:
                if len(params) == param_count:
                    cursor.execute(sql, tuple(params) if isinstance(params, list) else params)
                elif len(params) > param_count:
                    # 参数太多，只使用前 param_count 个
                    cursor.execute(sql, tuple(params[:param_count]))
                else:
                    # 参数不够，生成缺失的参数
                    wl_param = HyBench_Parameter()
                    missing_count = param_count - len(params)
                    missing_params = generate_random_params(missing_count, query_type, query_name, wl_param)
                    all_params = list(params) + missing_params
                    cursor.execute(sql, tuple(all_params))
            else:
                cursor.execute(sql)
            
            # 确保所有查询结果都被读取
            if sql.strip().upper().startswith('SELECT'):
                try:
                    return cursor.fetchall()
                except Exception as e:
                    # 如果没有结果，返回空列表
                    return []
            return None
            
    except Exception as e:
        print(f"Error executing {query_name}: {e}")
        raise


def generate_ap_query(sqls, max_qry_cnt):
    """生成并执行 AP 查询"""
    wl_param = HyBench_Parameter()
    qry_cnt = 0
    
    ap_queries = [f'AP-{i}' for i in range(1, 14)]
    
    while qry_cnt < max_qry_cnt:
        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:
                # 随机选择一个 AP 查询
                query_name = random.choice(ap_queries)
                if query_name not in sqls:
                    continue
                
                sql = sqls[query_name]
                param_count = count_parameters(sql)
                params = generate_random_params(param_count, 'AP', query_name, wl_param)
                
                query_idx = int(query_name.split('-')[1]) - 1
                hybench_stats.ap_query_cnt[query_idx] += 1
                
                start_time = time.time()
                try:
                    execute_sql_with_params(cur, sql, params, query_name, 'AP')
                    connection.commit()
                except Exception as e:
                    connection.rollback()
                    print(f"Error in {query_name}: {e}")
                    continue
                
                end_time = time.time()
                delay = end_time - start_time
                print(f"{query_name} Execution delay: {delay:.6f} seconds")
                hybench_stats.ap_query_lat_sum[query_idx] += delay
                
                qry_cnt += 1


def generate_tp_query(sqls, max_qry_cnt):
    """生成并执行 TP 查询"""
    wl_param = HyBench_Parameter()
    qry_cnt = 0
    
    tp_queries = [f'TP-{i}' for i in range(1, 19)]
    
    while qry_cnt < max_qry_cnt:
        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:
                query_name = random.choice(tp_queries)
                if query_name not in sqls:
                    continue
                
                sql = sqls[query_name]
                param_count = count_parameters(sql)
                params = generate_random_params(param_count, 'TP', query_name, wl_param)
                
                # 调试信息：检查参数数量
                if isinstance(sql, list):
                    actual_param_count = sum(stmt.count('?') for stmt in sql)
                else:
                    actual_param_count = sql.count('?')
                
                if len(params) != actual_param_count and query_name not in ['TP-13', 'TP-14', 'TP-15', 'TP-16']:
                    print(f"Warning: {query_name} param mismatch: generated={len(params)}, needed={actual_param_count}, counted={param_count}")
                
                query_idx = int(query_name.split('-')[1]) - 1
                hybench_stats.tp_query_cnt[query_idx] += 1
                
                start_time = time.time()
                try:
                    execute_sql_with_params(cur, sql, params, query_name, 'TP')
                    connection.commit()
                except Exception as e:
                    connection.rollback()
                    print(f"Error in {query_name}: {e}")
                    continue
                
                end_time = time.time()
                delay = end_time - start_time
                print(f"{query_name} Execution delay: {delay:.6f} seconds")
                hybench_stats.tp_query_lat_sum[query_idx] += delay
                
                qry_cnt += 1


def generate_at_query(sqls, max_qry_cnt):
    """生成并执行 AT 查询"""
    wl_param = HyBench_Parameter()
    qry_cnt = 0
    
    at_queries = ['AT-00', 'AT-0', 'AT-1', 'AT-2', 'AT-3', 'AT-3.1', 'AT-4', 'AT-4.1', 
                  'AT-5', 'AT-5.1', 'AT-6', 'AT-6.1']
    
    while qry_cnt < max_qry_cnt:
        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:
                query_name = random.choice(at_queries)
                if query_name not in sqls:
                    continue
                
                sql = sqls[query_name]
                param_count = count_parameters(sql)
                params = generate_random_params(param_count, 'AT', query_name, wl_param)
                
                if query_name not in hybench_stats.at_query_cnt:
                    hybench_stats.at_query_cnt[query_name] = 0
                    hybench_stats.at_query_lat_sum[query_name] = 0.0
                
                hybench_stats.at_query_cnt[query_name] += 1
                
                start_time = time.time()
                try:
                    execute_sql_with_params(cur, sql, params, query_name, 'AT')
                    connection.commit()
                except Exception as e:
                    connection.rollback()
                    print(f"Error in {query_name}: {e}")
                    continue
                
                end_time = time.time()
                delay = end_time - start_time
                print(f"{query_name} Execution delay: {delay:.6f} seconds")
                hybench_stats.at_query_lat_sum[query_name] += delay
                
                qry_cnt += 1


def generate_iq_query(sqls, max_qry_cnt):
    """生成并执行 IQ 查询"""
    wl_param = HyBench_Parameter()
    qry_cnt = 0
    
    iq_queries = ['IQ-1', 'IQ-2', 'IQ-3', 'IQ-4', 'IQ-5', 'IQ-5.1', 'IQ-6']
    
    while qry_cnt < max_qry_cnt:
        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:
                query_name = random.choice(iq_queries)
                if query_name not in sqls:
                    continue
                
                sql = sqls[query_name]
                param_count = count_parameters(sql)
                params = generate_random_params(param_count, 'IQ', query_name, wl_param)
                
                # 调试信息：检查参数数量
                if isinstance(sql, list):
                    actual_param_count = sum(stmt.count('?') for stmt in sql)
                else:
                    actual_param_count = sql.count('?')
                
                if len(params) != actual_param_count and query_name not in ['IQ-5.1', 'IQ-6']:
                    print(f"Warning: {query_name} param mismatch: generated={len(params)}, needed={actual_param_count}, counted={param_count}")
                
                if query_name not in hybench_stats.iq_query_cnt:
                    hybench_stats.iq_query_cnt[query_name] = 0
                    hybench_stats.iq_query_lat_sum[query_name] = 0.0
                
                hybench_stats.iq_query_cnt[query_name] += 1
                
                start_time = time.time()
                try:
                    result = execute_sql_with_params(cur, sql, params, query_name, 'IQ')
                    # 确保结果被读取（对于 SELECT 查询）
                    if result is not None and isinstance(result, list):
                        # 结果已经被读取
                        pass
                    connection.commit()
                except Exception as e:
                    connection.rollback()
                    print(f"Error in {query_name}: {e}")
                    continue
                
                end_time = time.time()
                delay = end_time - start_time
                print(f"{query_name} Execution delay: {delay:.6f} seconds")
                hybench_stats.iq_query_lat_sum[query_name] += delay
                
                qry_cnt += 1


def generate_workload(max_qry_cnt, ap_ratio=0.3, tp_ratio=0.3, at_ratio=0.2, iq_ratio=0.2):
    """生成混合工作负载"""
    wl_param = HyBench_Parameter()
    sqls = parse_toml_file(wl_param.sql_file_path)
    
    qry_cnt = 0
    
    while qry_cnt < max_qry_cnt:
        seed = random.random()
        
        if seed < ap_ratio:
            generate_ap_query(sqls, 1)
        elif seed < ap_ratio + tp_ratio:
            generate_tp_query(sqls, 1)
        elif seed < ap_ratio + tp_ratio + at_ratio:
            generate_at_query(sqls, 1)
        else:
            generate_iq_query(sqls, 1)
        
        qry_cnt += 1
    
    # 打印统计信息
    print_statistics()


def print_statistics():
    """打印统计信息"""
    print("\n=== HyBench Statistics ===")
    
    print("\nAP Query Statistics:")
    for i in range(13):
        if hybench_stats.ap_query_cnt[i] > 0:
            hybench_stats.ap_query_lat[i] = hybench_stats.ap_query_lat_sum[i] / hybench_stats.ap_query_cnt[i]
            print(f"AP-{i+1} cnt: {hybench_stats.ap_query_cnt[i]}, avg latency: {hybench_stats.ap_query_lat[i]:.6f}s")
    
    print("\nTP Query Statistics:")
    for i in range(18):
        if hybench_stats.tp_query_cnt[i] > 0:
            hybench_stats.tp_query_lat[i] = hybench_stats.tp_query_lat_sum[i] / hybench_stats.tp_query_cnt[i]
            print(f"TP-{i+1} cnt: {hybench_stats.tp_query_cnt[i]}, avg latency: {hybench_stats.tp_query_lat[i]:.6f}s")
    
    print("\nAT Query Statistics:")
    for query_name in hybench_stats.at_query_cnt:
        if hybench_stats.at_query_cnt[query_name] > 0:
            avg_lat = hybench_stats.at_query_lat_sum[query_name] / hybench_stats.at_query_cnt[query_name]
            print(f"{query_name} cnt: {hybench_stats.at_query_cnt[query_name]}, avg latency: {avg_lat:.6f}s")
    
    print("\nIQ Query Statistics:")
    for query_name in hybench_stats.iq_query_cnt:
        if hybench_stats.iq_query_cnt[query_name] > 0:
            avg_lat = hybench_stats.iq_query_lat_sum[query_name] / hybench_stats.iq_query_cnt[query_name]
            print(f"{query_name} cnt: {hybench_stats.iq_query_cnt[query_name]}, avg latency: {avg_lat:.6f}s")


def test_all_queries():
    """测试所有查询（每个查询执行一次）"""
    wl_param = HyBench_Parameter()
    sqls = parse_toml_file(wl_param.sql_file_path)
    
    # 测试 AP 查询
    print("Testing AP queries...")
    for i in range(1, 14):
        query_name = f'AP-{i}'
        if query_name not in sqls:
            continue
        
        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:
                sql = sqls[query_name]
                param_count = count_parameters(sql)
                params = generate_random_params(param_count, 'AP', query_name, wl_param)
                
                query_idx = i - 1
                hybench_stats.ap_query_cnt[query_idx] += 1
                
                start_time = time.time()
                try:
                    execute_sql_with_params(cur, sql, params, query_name, 'AP')
                    connection.commit()
                    end_time = time.time()
                    delay = end_time - start_time
                    print(f"{query_name} Execution delay: {delay:.6f} seconds")
                    hybench_stats.ap_query_lat_sum[query_idx] += delay
                except Exception as e:
                    connection.rollback()
                    print(f"Error in {query_name}: {e}")
    
    # 测试 TP 查询
    print("\nTesting TP queries...")
    for i in range(1, 19):
        query_name = f'TP-{i}'
        if query_name not in sqls:
            continue
        
        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:
                sql = sqls[query_name]
                param_count = count_parameters(sql)
                params = generate_random_params(param_count, 'TP', query_name, wl_param)
                
                query_idx = i - 1
                hybench_stats.tp_query_cnt[query_idx] += 1
                
                start_time = time.time()
                try:
                    execute_sql_with_params(cur, sql, params, query_name, 'TP')
                    connection.commit()
                    end_time = time.time()
                    delay = end_time - start_time
                    print(f"{query_name} Execution delay: {delay:.6f} seconds")
                    hybench_stats.tp_query_lat_sum[query_idx] += delay
                except Exception as e:
                    connection.rollback()
                    print(f"Error in {query_name}: {e}")
    
    print_statistics()


def test_toml_parsing():
    """测试 TOML 文件解析"""
    wl_param = HyBench_Parameter()
    sqls = parse_toml_file(wl_param.sql_file_path)
    
    print(f"Parsed {len(sqls)} SQL statements:")
    for key in sorted(sqls.keys()):
        sql = sqls[key]
        if isinstance(sql, list):
            print(f"{key}: {len(sql)} SQL statements")
        else:
            param_count = count_parameters(sql)
            print(f"{key}: {param_count} parameters")
    
    return sqls


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='HyBench Workload Test Script')
    parser.add_argument('--test-parse', action='store_true', help='Test TOML file parsing')
    parser.add_argument('--test-all', action='store_true', help='Test all queries once')
    parser.add_argument('--max-qry', type=int, default=100, help='Maximum number of queries to execute')
    parser.add_argument('--ap-ratio', type=float, default=0.3, help='AP query ratio')
    parser.add_argument('--tp-ratio', type=float, default=0.3, help='TP query ratio')
    parser.add_argument('--at-ratio', type=float, default=0.2, help='AT query ratio')
    parser.add_argument('--iq-ratio', type=float, default=0.2, help='IQ query ratio')
    
    args = parser.parse_args()
    
    if args.test_parse:
        # 测试 TOML 解析
        test_toml_parsing()
    elif args.test_all:
        # 测试所有查询
        test_all_queries()
    else:
        # 生成混合工作负载
        total_ratio = args.ap_ratio + args.tp_ratio + args.at_ratio + args.iq_ratio
        if abs(total_ratio - 1.0) > 0.01:
            print(f"Warning: Ratios sum to {total_ratio}, normalizing...")
            args.ap_ratio /= total_ratio
            args.tp_ratio /= total_ratio
            args.at_ratio /= total_ratio
            args.iq_ratio /= total_ratio
        
        generate_workload(
            max_qry_cnt=args.max_qry,
            ap_ratio=args.ap_ratio,
            tp_ratio=args.tp_ratio,
            at_ratio=args.at_ratio,
            iq_ratio=args.iq_ratio
        )

