import mysql.connector
import os
import time
import glob
from typing import List, Tuple
import random
import threading
import queue

def get_connection(autocommit: bool = True):
    db_conf = {
        "host": "",
        "port": 4000,
        "user": "root",
        "password": "",
        "database": "",
        "autocommit": autocommit,
        "use_pure": True
    }
    # db_conf = {
    #     "host": "10.77.110.144",
    #     "port": 4000,
    #     "user": "root",
    #     "password": "",
    #     "database": "oss",
    #     "autocommit": autocommit,
    #     "use_pure": True
    # }    
    return mysql.connector.connect(**db_conf)

def read_sql_files(directory: str) -> List[str]:
    return sorted(glob.glob(os.path.join(directory, "*.sql")))

def execute_sql_file(cur, sql_file: str) -> Tuple[str, float]:
    try:
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read().strip()
        if not sql_content:
            return os.path.basename(sql_file), 0.0
        
        # 替换表名
        # sql_content = sql_content.replace('github_events', 'sample_github_events')
        sql_content = sql_content.replace('github_users', 'github_users_partitioned')
        sql_content = sql_content.replace('github_repos', 'github_repos_partitioned')

        start_time = time.time()
        cur.execute(sql_content)
        # if sql_content.strip().upper().startswith('SELECT'):
        cur.fetchall()
        end_time = time.time()
        return os.path.basename(sql_file), end_time - start_time
    except Exception as e:
        print(f"执行SQL文件 {sql_file} 时出错: {str(e)}")
        return os.path.basename(sql_file), -1.0

def update_github_events_additions_by_id(conn, cur, event_id):
    sql = (
        "UPDATE github_users_partitioned "
        "SET is_bot = CASE WHEN is_bot = 0 THEN 0 ELSE 1 END "
        "WHERE id = %s"
    )
    start_time = time.time()
    cur.execute(sql, (event_id,))
    conn.commit()
    end_time = time.time()
    return end_time - start_time

# 线程任务
def thread_worker(tp_ratio, ap_ratio, ap_limit, ap_sql_files, event_ids, stats, stats_lock):
    import random
    conn = get_connection(autocommit=False)
    cur = conn.cursor()
    ap_count = 0
    tp_count = 0
    ap_times = {os.path.basename(f): [] for f in ap_sql_files}
    total_tp_time = 0.0
    event_id = 20599322522
    ap_sql_idx = 0  # 顺序索引
    ap_sql_len = len(ap_sql_files)
    while ap_count < ap_limit:
        # 决定执行tp还是ap
        if random.random() < tp_ratio / (tp_ratio + ap_ratio):
            # 执行TP
            print(f"执行TP事务, event_id: {event_id}")
            event_id = random.choice(event_ids)
            tp_time = update_github_events_additions_by_id(conn, cur, event_id)
            tp_count += 1
            total_tp_time += tp_time
        else:
            # 顺序执行AP
            sql_file = ap_sql_files[ap_sql_idx % ap_sql_len]
            ap_sql_idx += 1
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_content = f.read().strip()
            start_time = time.time()
            cur.execute(sql_content)
            print(f"执行AP事务, sql_file: {sql_file}")
            try:
                cur.fetchall()
            except Exception:
                pass
            end_time = time.time()
            ap_times[os.path.basename(sql_file)].append(end_time - start_time)
            ap_count += 1
    cur.close()
    conn.close()
    # 线程安全统计
    with stats_lock:
        stats['tp_count'] += tp_count
        stats['tp_total_time'] += total_tp_time
        for k, v in ap_times.items():
            stats['ap_counts'][k] = stats['ap_counts'].get(k, 0) + len(v)
            stats['ap_times'][k] = stats['ap_times'].get(k, []) + v

def run_tp_ap_mix(num_threads, tp_ratio, ap_ratio, ap_limit_per_thread, ap_sql_dir, event_ids):
    ap_sql_files = read_sql_files(ap_sql_dir)
    stats = {
        'tp_count': 0,
        'tp_total_time': 0.0,
        'ap_counts': {},
        'ap_times': {},
    }
    stats_lock = threading.Lock()
    threads = []
    import time
    start_wall = time.time()
    for _ in range(num_threads):
        t = threading.Thread(target=thread_worker, args=(tp_ratio, ap_ratio, ap_limit_per_thread, ap_sql_files, event_ids, stats, stats_lock))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    end_wall = time.time()
    wall_time = end_wall - start_wall
    # 输出统计
    print(f"\n所有线程总体执行时间(wall time): {wall_time:.4f} 秒")
    print("TP事务总数:", stats['tp_count'])
    print("TP总执行时间(s):", stats['tp_total_time'])
    if stats['tp_count'] > 0:
        print("TP平均吞吐(次/秒):", stats['tp_count'] / stats['tp_total_time'] if stats['tp_total_time'] > 0 else 0)
    # 统计AP总执行时间
    ap_total_time = sum([sum(times) for times in stats['ap_times'].values()])
    ap_total_count = sum([len(times) for times in stats['ap_times'].values()])
    print("AP总执行时间(s):", ap_total_time)
    print("AP总执行次数:", ap_total_count)
    print("\nAP SQL统计:")
    for k in sorted(stats['ap_counts']):
        count = stats['ap_counts'][k]
        times = stats['ap_times'][k]
        avg_time = sum(times) / len(times) if times else 0
        print(f"{k:<30} 执行次数: {count:<5} 平均延时: {avg_time:.4f} 秒")

def run():
    sql_directory = "/data3/dzh/project/grep/dev/OSS/workload"
    sql_files = read_sql_files(sql_directory)
    if not sql_files:
        print(f"未找到SQL文件: {sql_directory}")
        return

    results = []
    with get_connection(autocommit=True) as connection:
        with connection.cursor() as cur:
            # cur.execute(sql)
            # rows = cur.fetchall()
            # with open('execution plan.txt', 'w', encoding='utf-8') as f:
            #     for row in rows:
            #         # 将每一行转为字符串并写入文件，每行一个结果
            #         f.write(str(row) + '\n')        

            for sql_file in sql_files:
                filename, execution_time = execute_sql_file(cur, sql_file)
                results.append((filename, execution_time))
                if execution_time >= 0:
                    print(f"{filename:<30} {execution_time:.4f} 秒")
                else:
                    print(f"{filename:<30} 执行失败")

    # 统计
    times = [r[1] for r in results if r[1] >= 0]
    if times:
        print("=" * 60)
        print(f"平均执行时间: {sum(times)/len(times):.4f} 秒")
    else:
        print("没有成功执行的SQL文件")

def run_random_queries(num_queries: int):
    sql_directory = "/data3/dzh/project/grep/dev/OSS/workload"
    sql_files = read_sql_files(sql_directory)
    if not sql_files:
        print(f"未找到SQL文件: {sql_directory}")
        return
    # 有放回抽样
    selected_files = random.choices(sql_files, k=num_queries)
    results = []
    with get_connection(autocommit=True) as connection:
        with connection.cursor() as cur:
            for sql_file in selected_files:
                filename, execution_time = execute_sql_file(cur, sql_file)
                results.append((filename, execution_time))
                if execution_time >= 0:
                    print(f"{filename:<30} {execution_time:.4f} 秒")
                else:
                    print(f"{filename:<30} 执行失败")
    # 统计
    times = [r[1] for r in results if r[1] >= 0]
    if times:
        print("=" * 60)
        print(f"平均执行时间: {sum(times)/len(times):.4f} 秒")
    else:
        print("没有成功执行的SQL文件")

def run_queries_in_rounds(num_rounds: int):
    sql_directory = "/data3/dzh/project/grep/dev/OSS/workload"
    sql_files = read_sql_files(sql_directory)
    if not sql_files:
        print(f"未找到SQL文件: {sql_directory}")
        return
    results = []  # [(filename, execution_time)]
    with get_connection(autocommit=True) as connection:
        with connection.cursor() as cur:
            for round_idx in range(num_rounds):
                print(f"\n=== 第{round_idx+1}轮 ===")
                for sql_file in sql_files:
                    filename, execution_time = execute_sql_file(cur, sql_file)
                    results.append((filename, execution_time))
                    if execution_time >= 0:
                        print(f"{filename:<30} {execution_time:.4f} 秒")
                    else:
                        print(f"{filename:<30} 执行失败")
    # 统计
    from collections import defaultdict
    sql_times = defaultdict(list)  # filename -> [times]
    for filename, exec_time in results:
        if exec_time >= 0:
            sql_times[filename].append(exec_time)
    print("=" * 60)
    all_times = []
    for filename in sorted(sql_times):
        times = sql_times[filename]
        avg_time = sum(times) / len(times) if times else 0
        all_times.extend(times)
        print(f"{filename:<30} 平均执行时间: {avg_time:.4f} 秒 (执行 {len(times)} 次)")
    if all_times:
        print("-" * 60)
        print(f"所有SQL的总平均执行时间: {sum(all_times)/len(all_times):.4f} 秒")
    else:
        print("没有成功执行的SQL文件")

if __name__ == "__main__":
    # run()
    # run_random_queries(100)
    # run_queries_in_rounds(5)
    

    # def get_some_event_ids(limit=1000):
    #     ids = []
    #     try:
    #         with get_connection(autocommit=True) as conn:
    #             with conn.cursor() as cur:
    #                 cur.execute(f"SELECT id FROM github_users_partitioned LIMIT {limit}")
    #                 ids = [row[0] for row in cur.fetchall()]
    #     except Exception as e:
    #         print(f"获取event_ids失败: {e}")
    #     return ids

    # event_ids = get_some_event_ids(1000)
    event_ids = [1234]
    if event_ids:
        run_tp_ap_mix(
            num_threads=8,           # 线程数
            tp_ratio=1,              # TP:AP比例（如7:3）
            ap_ratio=1,
            ap_limit_per_thread=30,  # 每线程最多AP次数
            ap_sql_dir="/data3/dzh/project/grep/dev/OSS/workload",
            event_ids=event_ids
        )
    else:
        print("未能获取到可用的event_ids, TP事务无法执行。")