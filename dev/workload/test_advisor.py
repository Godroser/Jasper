import threading
import random
import string
import sys
import os
import time
from datetime import datetime

sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev"))

import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from config import Config
from collector import replica_progresss_collector
from collector import sync_metrics_collector

def get_connection(autocommit: bool = True) -> MySQLConnection:
  config = Config()
  db_conf = {
    "host": config.TIDB_HOST,
    "port": config.TIDB_PORT,
    "user": config.TIDB_USER,
    "password": config.TIDB_PASSWORD,
    "database": "ch_test", #config.TIDB_DB_NAME,
    "autocommit": autocommit,
    # mysql-connector-python will use C extension by default,
    # to make this example work on all platforms more easily,
    # we choose to use pure python implementation.
    "use_pure": True
  }

  if config.ca_path:
    db_conf["ssl_verify_cert"] = True
    db_conf["ssl_verify_identity"] = True
    db_conf["ssl_ca"] = config.ca_path
  return mysql.connector.connect(**db_conf)

class Workload_Parameter:
  def __init__(self):
    self.max_w_id = 4
    self.max_d_id = 10
    self.max_c_id = 3000
    self.max_stock_id = 100000
    self.max_o_id = 3223
    self.max_item = 100000
    self.max_nation = 24
    self.max_supplier_id = 10000
    self.max_region_id = 4
    self.max_ol_cnt = 1 # max item cnt in an order
    self.max_ol_quantity = 10 #max item quatity in an order
    self.max_carrier_id = 10 # max carrier id in delivery txn
    self.max_stock_cnt = 5 # max order cnt in Stock-Level txn
    self.quantity_threshold = 10 # threshold of item quantity in Stock-Level txn

    self.local_new_order_ratio = 0.9 #default 0.9
    self.lcoal_payment_ratio = 0.85 #default 0.85

    self.new_order_ratio = 0.45
    self.payment_ratio = 0.43
    self.order_status_ratio = 0.04
    self.delivery_ratio = 0.04
    self.stock_level_ratio = 0.04

    self.sql_file_path = 'workloadd_rewrite_0325.sql'
    # self.sql_file_path = 'workloadd.sql'
    self.sql_date_min = '2024-10-23 17:00:00'    # used in ap select
    self.sql_date_max = '2025-10-23 17:00:00'
    self.sql_date_mid = '2024-10-28 17:00:00'
    # Default
    # New-Order: 45%
    # Payment: 43%
    # Order-Status: 4%
    # Delivery: 4%
    # Stock-Level: 4% 


class Workload_Statistics:
  def __init__(self):
    self.neworder_cnt = 0
    self.neworder_lat = 0.0
    self.neworder_lat_sum = 0.0    
    self.payment_cnt = 0
    self.payment_lat = 0.0
    self.payment_lat_sum = 0.0
    self.orderstatus_cnt = 0
    self.orderstatus_lat = 0.0
    self.orderstatus_lat_sum = 0.0    
    self.delivery_cnt = 0
    self.delivery_lat = 0.0
    self.delivery_lat_sum = 0.0    
    self.stocklevel_cnt = 0
    self.stocklevel_lat = 0.0
    self.stocklevel_lat_sum = 0.0    

    self.query_cnt = [0] * 22
    self.query_lat = [0.0] * 22
    self.query_lat_sum = [0.0] * 22

wl_stats = Workload_Statistics()

def generate_ap(max_qry_cnt):
  wl_param = Workload_Parameter()
  qry_cnt = 0

  with open(wl_param.sql_file_path, 'r') as file:
    sql_script = file.read()
    sqls = [statement.strip() for statement in sql_script.split(';') if statement.strip()]

  while True:  
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur: 
        sql_no = random.randint(1, 22)
        wl_stats.query_cnt[sql_no-1] += 1

        start_time = time.time()
        cur.execute(sqls[sql_no-1])
        print("Query {}".format(sql_no), end=' ')
        
        end_time = time.time()
        cur.fetchall()
        delay = end_time - start_time
        print(f"Execution delay: {delay:.6f} seconds")   
        wl_stats.query_lat_sum[sql_no-1] += delay
        
        qry_cnt += 1

        if qry_cnt >= max_qry_cnt:
          break

## 顺次测试所有query
def test_ap(max_qry_cnt):
  wl_param = Workload_Parameter()
  qry_cnt = 0

  with open(wl_param.sql_file_path, 'r') as file:
    sql_script = file.read()
    sqls = [statement.strip() for statement in sql_script.split(';') if statement.strip()]

  while True:  
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur: 
        for sql_no in range(1, 23):
          wl_stats.query_cnt[sql_no-1] += 1
          
          start_time = time.time()
          cur.execute(sqls[sql_no-1])
          print("Query {}".format(sql_no), end=' ')
          
          end_time = time.time()
          cur.fetchall()
          delay = end_time - start_time
          print(f"Execution delay: {delay:.6f} seconds")   
          wl_stats.query_lat_sum[sql_no-1] += delay
          
          qry_cnt += 1

        if qry_cnt >= max_qry_cnt:
          break

  print("\n")
  print("Summary:")

  for i in range(22):
    if wl_stats.query_cnt[i] == 0:
      wl_stats.query_lat[i] = 0
    else:
      wl_stats.query_lat[i] = wl_stats.query_lat_sum[i] / wl_stats.query_cnt[i]
    print("Query {} cnt:{}, latency (avg):{:.6f}s".format(i+1, wl_stats.query_cnt[i], wl_stats.query_lat[i]))

  for i in range(22):
    print(wl_stats.query_lat[i])   


class TP_Workload_Genrator:
  def __init__(self, warehouse_number):
    #self.ratio = ratio  #tp-ap ratio
    self.warehouse_number = warehouse_number


  def generate_new_order(self):
    
    wl_param = Workload_Parameter()

    # New-Order
    d_id = random.randint(1, wl_param.max_d_id)
    w_id = random.randint(1, wl_param.max_w_id)
    c_id = random.randint(1, wl_param.max_c_id)
    o_ol_cnt = random.randint(1,wl_param.max_ol_cnt)  # order item count
    ol_i_id = [0] * o_ol_cnt  # item id
    ol_supply_w_id = [0] * o_ol_cnt  # supply warehouse id
    ol_quantity = [0] * o_ol_cnt  # item quantity
    o_all_local = wl_param.local_new_order_ratio


    for i in range(o_ol_cnt):
      ol_i_id[i] = random.randint(1, wl_param.max_item)


    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:
        cur.execute("start transaction;")

        sql_new_order1 = """
          SET @d_next_o_id =
          (SELECT d_next_o_id 
          FROM district_part2 
          WHERE d_id = {} AND d_w_id = {});
        """.format(d_id, w_id)
        #print(sql_new_order1)
        #print('sql_new_order1')  
        cur.execute(sql_new_order1)

        sql_new_order2="""
          UPDATE district_part2 
          SET d_next_o_id = d_next_o_id + 1 
          WHERE d_id = {} AND d_w_id = {};
        """.format(d_id, w_id)
        #print(sql_new_order2)
        #print('sql_new_order2')  
        cur.execute(sql_new_order2)


        #####o_all_local ??
        sql_new_order3="""
          INSERT INTO orders_part2 (o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local, o_id, o_d_id, o_w_id)
          VALUES ({}, NOW(), NULL, 1, {}, @d_next_o_id, {}, {});
        """.format(c_id, o_ol_cnt, d_id, w_id)
        cur.execute(sql_new_order3) 

        sql_new_order3="""
          INSERT INTO orders_part1  (o_id, o_d_id, o_w_id)
          VALUES (@d_next_o_id, {}, {});
        """.format(d_id, w_id, o_ol_cnt)
        cur.execute(sql_new_order3)         


        sql_new_order4="""
          INSERT INTO new_order_part2 (no_o_id, no_d_id, no_w_id)
          VALUES (@d_next_o_id, {}, {});
        """.format(d_id, w_id)
        #print(sql_new_order4)
        #print('sql_new_order4')  
        cur.execute(sql_new_order4)


        is_local_new_order = random.random()
        if is_local_new_order < wl_param.local_new_order_ratio:
          for i in range(o_ol_cnt):
            ol_supply_w_id[i] = random.randint(1, wl_param.max_w_id)
        else:
          for i in range(o_ol_cnt):
            ol_supply_w_id[i] = w_id


        for i in range(o_ol_cnt):
          sql_new_order5 = """    
            SET @s_quantity = (SELECT s_quantity FROM stock_part2 WHERE s_w_id = {} AND s_i_id = {});
          """.format(ol_supply_w_id[i], ol_i_id[i])
          #print(sql_new_order5)
          #print('sql_new_order5')  

          cur.execute(sql_new_order5)

          sql_new_order6 = "select @s_quantity;"

          cur.execute(sql_new_order6)
          s_quantity = cur.fetchall()[0][0]
          #print(s_quantity)

          if s_quantity >= ol_quantity[i]:
            s_quantity = s_quantity - ol_quantity[i]
          else:
            s_quantity = s_quantity + 100 - ol_quantity[i]

          if is_local_new_order < o_all_local: ## local warehouse
            sql_new_order7 = """
              UPDATE stock_part2
              SET s_quantity = {}, s_ytd = s_ytd + {}, s_order_cnt = s_order_cnt + 1
              WHERE s_w_id = {} AND s_i_id = {};
            """.format(s_quantity, ol_quantity[i], ol_supply_w_id[i], ol_i_id[i])
          else:   ## remote warehouse
            sql_new_order7 = """
              UPDATE stock_part2
              SET s_quantity = {}, s_ytd = s_ytd + {}, s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + 1
              WHERE s_w_id = {} AND s_i_id = {};
            """.format(s_quantity, ol_quantity[i], ol_supply_w_id[i], ol_i_id[i])
          #print(sql_new_order7)
          cur.execute(sql_new_order7)

          sql_new_order8 = """
            SET @i_price =
            (SELECT i_price
            FROM item_part2
            WHERE i_id = {});   
          """.format(ol_i_id[i])
          #print(sql_new_order8)
          cur.execute(sql_new_order8)
          
          sql_new_order9 = "select @i_price;"
          cur.execute(sql_new_order9)
          i_price = cur.fetchall()[0][0]


          ol_dist_info = "".join(random.choices(string.ascii_uppercase, k=24))
          sql_new_order10 = """
          INSERT INTO order_line_part2 (ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info, ol_o_id, ol_d_id, ol_w_id, ol_number)
          VALUES ({}, {}, NULL, {}, {}, '{}', @d_next_o_id, {}, {}, {});
          """.format(ol_i_id[i], ol_supply_w_id[i], ol_quantity[i], i_price * ol_quantity[i], ol_dist_info, d_id, w_id, i+1)
          #print(sql_new_order10)
          cur.execute(sql_new_order10)


        cur.execute("COMMIT;")
        print("New-Order complete! Warehouse {}, District {}, Customer {}".format(w_id, d_id, c_id), end=' ')

# """
# -- 假设事务输入的参数
# -- w_id: 仓库ID, d_id: 区域ID, c_id: 客户ID
# -- o_ol_cnt: 订单中商品项的数量, o_all_local: 是否全部商品来自本地
# -- item_data: 包含每个商品项的数据，包括 ol_i_id（商品ID）、 ol_supply_w_id（供应仓库ID）、 ol_quantity（商品数量）

# START TRANSACTION;

# -- Step 1: 获取仓库税率
# SELECT w_tax INTO @w_tax FROM warehouse WHERE w_id = w_id;

# -- Step 2: 获取地区税率和下一个订单ID
# SELECT d_tax, d_next_o_id
# INTO @d_tax, @d_next_o_id
# FROM district
# WHERE d_w_id = w_id AND d_id = d_id;

# -- 更新下一个订单ID
# UPDATE district
# SET d_next_o_id = d_next_o_id + 1
# WHERE d_w_id = w_id AND d_id = d_id;

# -- Step 3: 获取客户折扣和信用信息
# SELECT c_discount, c_credit, c_last
# INTO @c_discount, @c_credit, @c_last
# FROM customer
# WHERE c_w_id = w_id AND c_d_id = d_id AND c_id = c_id;

# -- Step 4: 插入订单
# INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)
# VALUES (@d_next_o_id, d_id, w_id, c_id, NOW(), NULL, o_ol_cnt, o_all_local);

# -- Step 5: 插入新订单标识
# INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
# VALUES (@d_next_o_id, d_id, w_id);

# -- Step 6: 为每个订单项处理商品库存、价格，并插入订单项记录
# -- 遍历每个商品项
# SET @total_amount = 0;

# -- 伪代码形式，实际需要在应用层遍历 item_data 并依次执行以下语句
# FOR EACH item IN item_data DO
#     SET @ol_number = item.ol_number;
#     SET @ol_i_id = item.ol_i_id;
#     SET @ol_supply_w_id = item.ol_supply_w_id;
#     SET @ol_quantity = item.ol_quantity;

#     -- 检查商品信息
#     SELECT i_price, i_name, i_data
#     INTO @i_price, @i_name, @i_data
#     FROM item
#     WHERE i_id = @ol_i_id;

#     -- 获取库存信息
#     SELECT s_quantity, s_data, s_ytd, s_order_cnt, s_remote_cnt
#     INTO @s_quantity, @s_data, @s_ytd, @s_order_cnt, @s_remote_cnt
#     FROM stock
#     WHERE s_w_id = @ol_supply_w_id AND s_i_id = @ol_i_id;

#     -- 更新库存数量
#     IF @s_quantity > @ol_quantity THEN
#         SET @s_quantity = @s_quantity - @ol_quantity;
#     ELSE
#         SET @s_quantity = @s_quantity + 91 - @ol_quantity;
#     END IF;

#     -- 更新 stock 表
#     UPDATE stock
#     SET s_quantity = @s_quantity,
#         s_ytd = s_ytd + @ol_quantity,
#         s_order_cnt = s_order_cnt + 1,
#         s_remote_cnt = IF(@ol_supply_w_id != w_id, s_remote_cnt + 1, s_remote_cnt)
#     WHERE s_w_id = @ol_supply_w_id AND s_i_id = @ol_i_id;

#     -- 计算订单项金额
#     SET @ol_amount = @ol_quantity * @i_price;
#     SET @total_amount = @total_amount + @ol_amount;

#     -- 插入订单项记录
#     INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info)
#     VALUES (@d_next_o_id, d_id, w_id, @ol_number, @ol_i_id, @ol_supply_w_id, NULL, @ol_quantity, @ol_amount, SUBSTRING(s_data, 1, 24));
# END FOR;

# -- Step 7: 计算总金额并应用折扣
# SET @total_amount = @total_amount * (1 - @c_discount) * (1 + @w_tax + @d_tax);

# COMMIT;
# """


  def generate_payment(self):
    wl_param = Workload_Parameter()
    local_payment_ratio = wl_param.lcoal_payment_ratio

    # Payment
    c_d_id = random.randint(1, wl_param.max_d_id)
    c_w_id = random.randint(1, wl_param.max_w_id)
    c_id = random.randint(1, wl_param.max_c_id)
    payment_amount = random.uniform(1.0, 1000.0)

    if random.random() < local_payment_ratio: ## local payemnt
      w_id = c_w_id
      d_id = c_d_id
    else:
      while True:
        w_id = random.randint(1, wl_param.max_w_id)
        if w_id != c_w_id:
          break
      while True:
        d_id = random.randint(1, wl_param.max_d_id)
        if d_id != c_d_id:
          break


    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur: 
        cur.execute("start transaction;")   

        sql_payment0 = """
          SELECT c_id, c_balance, c_ytd_payment, c_payment_cnt
          FROM customer_part2
          WHERE c_w_id = {}
            AND c_d_id = {}
          ORDER BY c_first;
        """.format(c_w_id, c_d_id)
        cur.execute(sql_payment0)
        cur.fetchall()
        
        sql_payment1 = """
          UPDATE customer_part2
          SET c_balance = c_balance - {},
              c_ytd_payment = c_ytd_payment + {},
              c_payment_cnt = c_payment_cnt + 1
          WHERE c_w_id = {}
            AND c_d_id = {}
            AND c_id = {};
        """.format(payment_amount, payment_amount, c_w_id, c_d_id, c_id)
        #print(sql_payment1)
        cur.execute(sql_payment1)

        sql_payment2 = """
          UPDATE district_part2
          SET d_ytd = d_ytd + {}
          WHERE d_w_id = {}
            AND d_id = {};
        """.format(payment_amount, w_id, d_id)
        #print(sql_payment2)
        cur.execute(sql_payment2)

        sql_payment3 = """
          UPDATE warehouse_part2
          SET w_ytd = w_ytd + {}
          WHERE w_id = {};
        """.format(payment_amount, w_id)
        #print(sql_payment3)
        cur.execute(sql_payment3)

        
        characters = string.ascii_letters + string.digits
        # random h_data
        h_data = ''.join(random.choices(characters, k=24))

        sql_payment4 = """
          INSERT INTO history_part1 (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_data)
          VALUES ({}, {}, {}, {}, {}, NOW(), '{}');
        """.format(c_id, c_d_id, c_w_id, d_id, w_id, h_data)
        #print(sql_payment4)
        cur.execute(sql_payment4)

        sql_payment4 = """
          INSERT INTO history_part2 (h_amount, h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date)
          VALUES ({}, {}, {}, {}, {}, {}, NOW());
        """.format(payment_amount, c_id, c_d_id, c_w_id, d_id, w_id)
        #print(sql_payment4)
        cur.execute(sql_payment4)        

        cur.execute("commit;")
        print("Payment Done! Warehouse {}, District {}, Customer {}".format(w_id, d_id, c_id), end=' ')


# """
# -- 更新仓库收入
# UPDATE warehouse 
# SET w_ytd = w_ytd + ? 
# WHERE w_id = ?;

# -- 更新区域收入
# UPDATE district 
# SET d_ytd = d_ytd + ? 
# WHERE d_id = ? AND d_w_id = ?;

# -- 获取客户信息
# SELECT c_id, c_balance, c_ytd_payment, c_payment_cnt, c_credit 
# FROM customer 
# WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;

# -- 更新客户的支付信息
# UPDATE customer 
# SET c_balance = c_balance - ?, 
#     c_ytd_payment = c_ytd_payment + ?, 
#     c_payment_cnt = c_payment_cnt + 1 
# WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;

# -- 插入支付历史记录
# INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data)
# VALUES (?, ?, ?, ?, ?, ?, ?, ?);
# """

  
  def generate_order_status(self):
    wl_param = Workload_Parameter()

    # Order_Status
    c_d_id = random.randint(1, wl_param.max_d_id)
    c_w_id = random.randint(1, wl_param.max_w_id)
    c_id = random.randint(1, wl_param.max_c_id)


    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur: 
        cur.execute("start transaction;")   

        sql_order_status1 = """
          SELECT c_id, c_balance, c_first, c_middle, c_last
          FROM customer_part2
          WHERE c_w_id = {}
            AND c_d_id = {}
            AND c_id = {};
        """.format(c_w_id, c_d_id, c_id)
        #print(sql_order_status1)
        cur.execute(sql_order_status1)
        cur.fetchall()

        sql_order_status2 = """
          SET @o_id =
          (SELECT o_id
          FROM orders_part2
          WHERE o_w_id = {}
            AND o_d_id = {}
            AND o_c_id = {}
          ORDER BY o_id DESC
          LIMIT 1);
        """.format(c_w_id, c_d_id, c_id)
        #print(sql_order_status2)
        cur.execute(sql_order_status2)
        cur.fetchall()

        sql_order_status3 = """
          SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d
          FROM order_line_part2
          WHERE ol_w_id = {}
            AND ol_d_id = {}
            AND ol_o_id = @o_id;
        """.format(c_w_id, c_d_id)
        #print(sql_order_status3)
        cur.execute(sql_order_status3)
        cur.fetchall()

        cur.execute("commit;")
        print("Order Status Done! Warehouse {}, District {}, Customer {}".format(c_w_id, c_d_id, c_id), end=' ')


# """
# -- 获取客户信息
# SELECT c_id, c_balance, c_first, c_middle, c_last 
# FROM customer 
# WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;

# -- 获取最新订单
# SELECT o_id, o_carrier_id, o_entry_d 
# FROM orders 
# WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? 
# ORDER BY o_id DESC 
# LIMIT 1;

# -- 获取订单行信息
# SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d 
# FROM order_line 
# WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?;
# """

  def generate_delivery(self):
    wl_param = Workload_Parameter()

    # Delivery
    d_id = random.randint(1, wl_param.max_d_id)
    w_id = random.randint(1, wl_param.max_w_id)
    carrier_id = random.randint(1, wl_param.max_carrier_id) 


    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur: 
        cur.execute("start transaction;")   

        sql_delivery1 = """
          SET @o_id =
          (SELECT o_id 
          FROM orders_part2
          WHERE o_w_id = {} AND o_d_id = {} AND o_carrier_id IS NULL 
          ORDER BY o_id ASC 
          LIMIT 1);
        """.format(w_id, d_id)
        #print(sql_delivery1)
        cur.execute(sql_delivery1)
        

        sql_delivery2 = """
          UPDATE orders_part2
          SET o_carrier_id = {}
          WHERE o_w_id = {}
            AND o_d_id = {}
            AND o_id = @o_id;
        """.format(w_id, d_id, carrier_id)
        #print(sql_delivery2)
        cur.execute(sql_delivery2)

        sql_delivery3 = """
          UPDATE order_line_part2
          SET ol_delivery_d = NOW()
          WHERE ol_w_id = {}
            AND ol_d_id = {}
            AND ol_o_id = @o_id;
        """.format(w_id, d_id)
        #print(sql_delivery3)
        cur.execute(sql_delivery3)

        sql_delivery4 = """
          SET @total_amount =
          (SELECT SUM(ol_amount) AS total_order_amount
          FROM order_line_part2
          WHERE ol_w_id = {}
            AND ol_d_id = {}
            AND ol_o_id = @o_id);
        """.format(w_id, d_id)
        #print(sql_delivery4)
        cur.execute(sql_delivery4)

        sql_delivery5 = """
          UPDATE customer_part2
          SET c_balance = c_balance + @total_amount
          WHERE c_w_id = {}
            AND c_d_id = {}
            AND c_id = (
              SELECT o_c_id
              FROM orders_part2
              WHERE o_w_id = {}
                AND o_d_id = {}
                AND o_id = @o_id);
        """.format(w_id, d_id, w_id, d_id)
        #print(sql_delivery5)
        cur.execute(sql_delivery5)

        cur.execute("commit;")
        print("Delivery Done! Warehouse {}, District {}, Carrier {}".format(w_id, d_id, carrier_id), end=' ')


# """
# -- 查找待发货的订单
# SELECT o_id 
# FROM orders 
# WHERE o_w_id = ? AND o_d_id = ? AND o_carrier_id IS NULL 
# ORDER BY o_id ASC 
# LIMIT 1;

# -- 更新订单的发货人
# UPDATE orders 
# SET o_carrier_id = ? 
# WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?;

# -- 更新订单行的发货日期
# UPDATE order_line 
# SET ol_delivery_d = ? 
# WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?;

# -- 更新客户的余额和支付次数
# UPDATE customer 
# SET c_balance = c_balance + ?, 
#     c_delivery_cnt = c_delivery_cnt + 1 
# WHERE c_id = ? AND c_w_id = ? AND c_d_id = ?;
# """


  def generate_stock_level(self):
    wl_param = Workload_Parameter()

    # Stock-level
    stock_cnt = random.randint(1, wl_param.max_stock_cnt)
    d_id = random.randint(1, wl_param.max_d_id)
    w_id = random.randint(1, wl_param.max_w_id)
    quantity_threshold = wl_param.quantity_threshold

    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur: 
        cur.execute("start transaction;")     

        sql_stock_level1 = """
          SELECT o_id
          FROM orders_part2
          WHERE o_w_id = {}
            AND o_d_id = {}
          ORDER BY o_id DESC
          LIMIT {};
        """.format(w_id, d_id, stock_cnt)
        #print(sql_stock_level1)
        cur.execute(sql_stock_level1)

        
        o_ids = cur.fetchall()
        o_id = [0] * len(o_ids)
        
        for i in range(len(o_ids)):
          o_id[i] = o_ids[i][0] 
        ol_o_id =  ', '.join(str(item) for item in o_id)
        #print(ol_o_id)

        sql_stock_level2 = """
          SELECT DISTINCT ol_i_id 
          FROM order_line_part2 
          WHERE ol_w_id = {}  
            AND ol_d_id = {} AND ol_o_id IN ({});
        """.format(w_id, d_id, ol_o_id)
        #print(sql_stock_level2)
        cur.execute(sql_stock_level2)
        
        
        i_ids = cur.fetchall()
        i_id = [0] * len(i_ids)
        for i in range(len(i_ids)):
          i_id[i] = i_ids[i][0]
        s_i_id =  ', '.join(str(item) for item in i_id)
        #print(type(s_i_id))
        
        if s_i_id == '':
          pass
        else:
          sql_stock_level3 = """
            SELECT COUNT(*)
            FROM stock_part2
            WHERE s_w_id = {}
              AND s_i_id IN ({})
              AND s_quantity < {};
          """.format(w_id, s_i_id, quantity_threshold)
          #print(sql_stock_level3)
          cur.execute(sql_stock_level3)
          cur.fetchall()

        cur.execute("commit;")
        print("Stock Level Done! Warehouse {}, District {}, Stock Count {}".format(w_id, d_id, stock_cnt), end=' ')

  # """
  # -- 获取最近的订单 ID 范围
  # SELECT o_id 
  # FROM orders 
  # WHERE o_w_id = ? AND o_d_id = ? 
  # ORDER BY o_id DESC 
  # LIMIT ?;

  # -- 获取低库存的库存项
  # SELECT COUNT(DISTINCT s_i_id) 
  # FROM order_line, stock 
  # WHERE ol_w_id = ? 
  #   AND ol_d_id = ? 
  #   AND ol_o_id BETWEEN ? AND ? 
  #   AND s_w_id = ? 
  #   AND s_i_id = ol_i_id 
  #   AND s_quantity < ?;
  # """

def generate_tp(max_txn_cnt):
  wl_param = Workload_Parameter()
  tp_wl_generator = TP_Workload_Genrator(wl_param.max_w_id)

  txn_cnt = 0
  
  start_sync_time, start_count = sync_metrics_collector.direct_get_tiflash_syncing_data_freshness_count()

  while True:
    seed = random.random()
    print("Txn {}:".format(txn_cnt), end='')
    start_time = time.time()

    if seed <= wl_param.new_order_ratio:
      tp_wl_generator.generate_new_order()
      wl_stats.neworder_cnt += 1

      end_time = time.time()
      delay = end_time - start_time
      print(f"Execution delay: {delay:.6f} seconds")    
      wl_stats.neworder_lat_sum += delay  
    elif seed <= wl_param.new_order_ratio + wl_param.payment_ratio:
      tp_wl_generator.generate_payment()
      wl_stats.payment_cnt += 1

      end_time = time.time()
      delay = end_time - start_time
      print(f"Execution delay: {delay:.6f} seconds") 
      wl_stats.payment_lat_sum += delay      
    elif seed <= wl_param.new_order_ratio + wl_param.payment_ratio + wl_param.order_status_ratio:
      tp_wl_generator.generate_order_status()
      wl_stats.orderstatus_cnt += 1

      end_time = time.time()
      delay = end_time - start_time
      print(f"Execution delay: {delay:.6f} seconds") 
      wl_stats.orderstatus_lat_sum += delay      
    elif seed <= 1 - wl_param.delivery_ratio:
      tp_wl_generator.generate_delivery()
      wl_stats.delivery_cnt += 1

      end_time = time.time()
      delay = end_time - start_time
      print(f"Execution delay: {delay:.6f} seconds") 
      wl_stats.delivery_lat_sum += delay      
    else:
      tp_wl_generator.generate_stock_level()
      wl_stats.stocklevel_cnt += 1

      end_time = time.time()
      delay = end_time - start_time
      print(f"Execution delay: {delay:.6f} seconds") 
      wl_stats.stocklevel_lat_sum += delay      
    

    
    txn_cnt += 1
    if txn_cnt >= max_txn_cnt:
      break

  ## GET SYNC TIME AND SYNC_COUNT
  # start_sync_time, start_count = sync_metrics_collector.direct_get_tiflash_syncing_data_freshness_count()

  # 定义日期时间格式
  time_format = '%Y-%m-%d %H:%M:%S'

  end_sync_time, end_count = sync_metrics_collector.direct_get_tiflash_syncing_data_freshness_count()

  start_sync_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(start_sync_time)))
  end_sync_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(end_sync_time)))
  # print(start_sync_time)
  # print(end_sync_time)
  sync_time = datetime.strptime(end_sync_time, time_format) - datetime.strptime(start_sync_time, time_format)
  # print("Sync LATENCY: ", sync_time)
  print("Sync DATA COUNT: ", int(end_count) - int(start_count))

## max_txn_cnt : max cnt of txn and qry; ratio[0,1]: tp ratio
def generate_workload(max_txn_cnt, ratio):
  txn_cnt = 0
  ## GET SYNC TIME AND SYNC_COUNT
  start_sync_time, start_count = sync_metrics_collector.direct_get_tiflash_syncing_data_freshness_count()

  while True:
    seed = random.random()
    if seed <= ratio:
      generate_tp(1)
    else:
      generate_ap(1)
    txn_cnt += 1
    if txn_cnt >= max_txn_cnt:
      break


  ## GET SYNC TIME AND SYNC_COUNT

  # 定义日期时间格式
  time_format = '%Y-%m-%d %H:%M:%S'

  end_sync_time, end_count = sync_metrics_collector.direct_get_tiflash_syncing_data_freshness_count()

  start_sync_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(start_sync_time)))
  end_sync_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(end_sync_time)))
  sync_time = datetime.strptime(end_sync_time, time_format) - datetime.strptime(start_sync_time, time_format)


  #check_replica_status('order_line')

  #Statistics
  if wl_stats.neworder_cnt == 0:
    wl_stats.neworder_lat = 0
  else:
    wl_stats.neworder_lat = wl_stats.neworder_lat_sum / wl_stats.neworder_cnt

  if wl_stats.payment_cnt == 0:
    wl_stats.payment_lat = 0  
  else:
    wl_stats.payment_lat = wl_stats.payment_lat_sum / wl_stats.payment_cnt
  
  if wl_stats.orderstatus_cnt == 0:
    wl_stats.orderstatus_lat = 0
  else:
    wl_stats.orderstatus_lat = wl_stats.orderstatus_lat_sum / wl_stats.orderstatus_cnt
  
  if wl_stats.delivery_cnt == 0:
    wl_stats.delivery_lat = 0
  else:
    wl_stats.delivery_lat = wl_stats.delivery_lat_sum / wl_stats.delivery_cnt
  
  if wl_stats.stocklevel_cnt == 0:
    wl_stats.stocklevel_lat = 0
  else:
    wl_stats.stocklevel_lat = wl_stats.stocklevel_lat_sum / wl_stats.stocklevel_cnt
  
  print("\n")
  print("Summary:")
  print("Txn New Order cnt:{}, latency (avg):{:.6f}s".format(wl_stats.neworder_cnt, wl_stats.neworder_lat))
  print("Txn Payment cnt:{}, latency (avg):{:.6f}s".format(wl_stats.payment_cnt, wl_stats.payment_lat))
  print("Txn Order Status cnt:{}, latency (avg):{:.6f}s".format(wl_stats.orderstatus_cnt, wl_stats.orderstatus_lat))
  print("Txn Delivery cnt:{}, latency (avg):{:.6f}s".format(wl_stats.delivery_cnt, wl_stats.delivery_lat))
  print("Txn Stock Level cnt:{}, latency (avg):{:.6f}s".format(wl_stats.stocklevel_cnt, wl_stats.stocklevel_lat))
  
  for i in range(22):
    if wl_stats.query_cnt[i] == 0:
      wl_stats.query_lat[i] = 0
    else:
      wl_stats.query_lat[i] = wl_stats.query_lat_sum[i] / wl_stats.query_cnt[i]
    print("Query {} cnt:{}, latency (avg):{:.6f}s".format(i+1, wl_stats.query_cnt[i], wl_stats.query_lat[i]))
  

  print("Sync LATENCY: ", sync_time)
  print("Sync DATA COUNT: ", int(end_count) - int(start_count))

  print("Query Latency:")
  for i in range(22):
    print(wl_stats.query_lat[i])  


# 测试事务执行期间的 query 延迟, 这里的tp的并发度是n
def test_query_latency_with_tp(max_txn_cnt, max_qry_cnt, n, m):
    """
    测试事务执行期间的 query 延迟。
    """
    start_sync_time, start_count = sync_metrics_collector.direct_get_tiflash_syncing_data_freshness_count()

    stop_event = threading.Event()  # 用于通知 query 线程停止

    def run_generate_tp():
        generate_tp(max_txn_cnt)
        stop_event.set()  # 事务执行完成后通知 query 线程停止

    def run_generate_ap():
        qry_cnt = 0
        while not stop_event.is_set():  # 检查是否需要停止
            generate_ap(1)
            qry_cnt += 1
            if qry_cnt >= max_qry_cnt:
                break

    # 开辟线程执行事务生成
    tp_threads = [threading.Thread(target=run_generate_tp) for _ in range(n)]
    # 开辟线程执行 query 生成
    ap_threads = [threading.Thread(target=run_generate_ap) for _ in range(m)]

    # 启动线程
    for tp_thread in tp_threads:
        tp_thread.start()

    for ap_thread in ap_threads:
        ap_thread.start()        

    # 等待线程完成
    for tp_thread in tp_threads:
        tp_thread.join()
    
    for ap_thread in ap_threads:
        ap_thread.join()

    end_sync_time, end_count = sync_metrics_collector.direct_get_tiflash_syncing_data_freshness_count()

    start_sync_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(start_sync_time)))
    end_sync_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(end_sync_time)))

    # 定义日期时间格式
    time_format = '%Y-%m-%d %H:%M:%S'    
    # print(start_sync_time)
    # print(end_sync_time)
    sync_time = datetime.strptime(end_sync_time, time_format) - datetime.strptime(start_sync_time, time_format)
    print("Sync LATENCY: ", sync_time)
    print("Sync DATA COUNT: ", int(end_count) - int(start_count))    

    # 输出统计信息  
    if wl_stats.neworder_cnt == 0:
      wl_stats.neworder_lat = 0
    else:
      wl_stats.neworder_lat = wl_stats.neworder_lat_sum / wl_stats.neworder_cnt

    if wl_stats.payment_cnt == 0:
      wl_stats.payment_lat = 0  
    else:
      wl_stats.payment_lat = wl_stats.payment_lat_sum / wl_stats.payment_cnt
    
    if wl_stats.orderstatus_cnt == 0:
      wl_stats.orderstatus_lat = 0
    else:
      wl_stats.orderstatus_lat = wl_stats.orderstatus_lat_sum / wl_stats.orderstatus_cnt
    
    if wl_stats.delivery_cnt == 0:
      wl_stats.delivery_lat = 0
    else:
      wl_stats.delivery_lat = wl_stats.delivery_lat_sum / wl_stats.delivery_cnt
    
    if wl_stats.stocklevel_cnt == 0:
      wl_stats.stocklevel_lat = 0
    else:
      wl_stats.stocklevel_lat = wl_stats.stocklevel_lat_sum / wl_stats.stocklevel_cnt

    print("Summary:")
    print("Txn New Order cnt:{}, latency (avg):{:.6f}s".format(wl_stats.neworder_cnt, wl_stats.neworder_lat))
    print("Txn Payment cnt:{}, latency (avg):{:.6f}s".format(wl_stats.payment_cnt, wl_stats.payment_lat))
    print("Txn Order Status cnt:{}, latency (avg):{:.6f}s".format(wl_stats.orderstatus_cnt, wl_stats.orderstatus_lat))
    print("Txn Delivery cnt:{}, latency (avg):{:.6f}s".format(wl_stats.delivery_cnt, wl_stats.delivery_lat))
    print("Txn Stock Level cnt:{}, latency (avg):{:.6f}s".format(wl_stats.stocklevel_cnt, wl_stats.stocklevel_lat))

    # 输出 22 条 query 的延迟统计
    print("\nQuery Latency Summary:")
    for i in range(22):
        if wl_stats.query_cnt[i] == 0:
            wl_stats.query_lat[i] = 0
        else:
            wl_stats.query_lat[i] = wl_stats.query_lat_sum[i] / wl_stats.query_cnt[i]
        print("Query {} cnt:{}, latency (avg):{:.6f}s".format(i + 1, wl_stats.query_cnt[i], wl_stats.query_lat[i]))

    for i in range(22):
        print(wl_stats.query_lat[i])


class TPBenchmark:
    def __init__(self, num_clients, duration_seconds):
        self.num_clients = num_clients
        self.duration_seconds = duration_seconds
        self.stop_event = threading.Event()
        self.latency_stats = {
            'new_order': [],
            'payment': [],
            'order_status': [],
            'delivery': [],
            'stock_level': []
        }
        self.txn_counts = {
            'new_order': 0,
            'payment': 0,
            'order_status': 0,
            'delivery': 0,
            'stock_level': 0
        }
        self.lock = threading.Lock()

    def run_client(self, client_id):
        wl_param = Workload_Parameter()
        tp_wl_generator = TP_Workload_Genrator(wl_param.max_w_id)
        
        while not self.stop_event.is_set():
            seed = random.random()
            start_time = time.time()
            
            if seed <= wl_param.new_order_ratio:
                tp_wl_generator.generate_new_order()
                txn_type = 'new_order'
            elif seed <= wl_param.new_order_ratio + wl_param.payment_ratio:
                tp_wl_generator.generate_payment()
                txn_type = 'payment'
            elif seed <= wl_param.new_order_ratio + wl_param.payment_ratio + wl_param.order_status_ratio:
                tp_wl_generator.generate_order_status()
                txn_type = 'order_status'
            elif seed <= 1 - wl_param.delivery_ratio:
                tp_wl_generator.generate_delivery()
                txn_type = 'delivery'
            else:
                tp_wl_generator.generate_stock_level()
                txn_type = 'stock_level'
            
            end_time = time.time()
            latency = end_time - start_time
            
            with self.lock:
                self.latency_stats[txn_type].append(latency)
                self.txn_counts[txn_type] += 1

    def calculate_percentile(self, latencies, percentile):
        if not latencies:
            return 0
        latencies.sort()
        index = int(len(latencies) * percentile / 100)
        return latencies[index]

    def run_benchmark(self):
        threads = []
        for i in range(self.num_clients):
            thread = threading.Thread(target=self.run_client, args=(i,))
            threads.append(thread)
            thread.start()

        time.sleep(self.duration_seconds)
        self.stop_event.set()

        for thread in threads:
            thread.join()

        # Calculate statistics
        total_txns = sum(self.txn_counts.values())
        tps = total_txns / self.duration_seconds

        results = {
            'tps': tps,
            'txn_counts': self.txn_counts,
            'latency_stats': {}
        }

        for txn_type in self.latency_stats:
            latencies = self.latency_stats[txn_type]
            if latencies:
                avg_latency = sum(latencies) / len(latencies)
                p50_latency = self.calculate_percentile(latencies, 50)
                p95_latency = self.calculate_percentile(latencies, 95)
                
                results['latency_stats'][txn_type] = {
                    'avg': avg_latency,
                    'p50': p50_latency,
                    'p95': p95_latency
                }

        return results

def run_tp_benchmark(num_clients=10, duration_seconds=60):
    """
    运行TP事务压测
    
    Args:
        num_clients: 并发客户端数量
        duration_seconds: 压测持续时间（秒）
    
    Returns:
        包含压测结果的字典
    """
    benchmark = TPBenchmark(num_clients, duration_seconds)
    results = benchmark.run_benchmark()
    
    # 打印结果
    print("\n=== TP Benchmark Results ===")
    print(f"Total TPS: {results['tps']:.2f}")
    print("\nTransaction Counts:")
    for txn_type, count in results['txn_counts'].items():
        print(f"{txn_type}: {count}")
    
    print("\nLatency Statistics (seconds):")
    for txn_type, stats in results['latency_stats'].items():
        print(f"\n{txn_type}:")
        print(f"  Average: {stats['avg']:.6f}")
        print(f"  50th percentile: {stats['p50']:.6f}")
        print(f"  95th percentile: {stats['p95']:.6f}")
    
    return results

# 使用示例
if __name__ == '__main__':
  # connection自定义测试的数据库
  # test_ap(88)
  # generate_workload(100, 1)

  # 每个线程测试100条txn，1000条sql，txn并发度是1, ap并发度是5
  test_query_latency_with_tp(300, 1000, 3, 1)

  # # 运行压测：10个客户端，持续60秒
  # results = run_tp_benchmark(num_clients=20, duration_seconds=60)
  # print(results)