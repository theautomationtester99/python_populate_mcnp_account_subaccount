import json
import sqlite3

import requests

from get_consignee_ids import get_consignee_id_list, get_consignee_id_list_by_fn_search_jnj
from utils import gen_rand1, gen_rand_alpha

# alist = get_consignee_id_list(600)
# print(len(alist))
# print(alist)

# alist = get_consignee_id_list_by_fn_search("")
# print(len(alist))
# print(alist)

# con = sqlite3.connect('account_names.db')
# sql = 'SELECT account_name FROM account_names ORDER BY RANDOM() LIMIT 1'
# cur = con.cursor()
# cur.execute(sql)
# record = cur.fetchall()
# print(record[0][0])

random_ucn_int = gen_rand1(1, 9999999)
ucn = f'{random_ucn_int:015d}'
print(ucn)

gen_rand_alpha(16)

