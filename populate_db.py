from jproperties import Properties

from db_functions import insert_one_record_accounts_table, insert_main_accounts_mapping, insert_sub_accounts_mapping, \
    get_all_sub_accounts, insert_consignee_account_mapping_table, delete_data_account_sub_consignee, \
    insert_main_accounts_with_self_mapping, insert_ten_consignee_per_account, \
    insert_ten_nonactive_consignee_per_account, get_all_sub_accounts_latest
from get_consignee_ids import get_consignee_id_list_by_fn_search_jnj, get_consignee_id_list_by_fn_search_qa
from utils import gen_rand1
from sys import exit

"""
Insert specified number of records into accounts table.

Out of total records, 1/10th of accounts will be added as main accounts in main_sub_account_mapping table.
Out of remaining, 50% will be added as sub accounts.
Remaining will be free or un mapped accounts.
"""


def to_map_accounts(no_accounts):
    sub_accounts_list = get_all_sub_accounts_latest(number_of_accounts)
    # print(sub_accounts_list)
    to_map_accounts_list = []
    temp_list = []
    a = 0
    while a < int(no_accounts):
        # print(sub_accounts_list)
        rand_acc = gen_rand1(0, len(sub_accounts_list)-1)
        print(rand_acc)
        temp_list.append(sub_accounts_list[rand_acc])
        print(temp_list)
        # to_map_accounts_list = [*set(temp_list)]
        to_map_accounts_list = []
        [to_map_accounts_list.append(x) for x in temp_list if x not in to_map_accounts_list]
        print(to_map_accounts_list)
        a = len(to_map_accounts_list)
    return to_map_accounts_list


configs = Properties()
with open('input.properties', 'rb') as config_file:
    configs.load(config_file)

number_of_accounts = int(configs.get("number_of_accounts_to_load").data)
populate_accounts_and_mapping = str(configs.get("populate_accounts_and_mapping").data)
populate_account_consignee_mapping = str(configs.get("populate_account_consignee_mapping").data)
consignee_fn_starts_with = str(configs.get("consignee_fn_starts_with").data)
no_of_consignees_to_map = int(configs.get("no_of_consignees_to_map").data)
no_accounts_per_consignee = int(configs.get("no_accounts_per_consignee").data)
delete_existing_data = str(configs.get("delete_existing_data").data)
main_sub_acc_self = str(configs.get("main_sub_acc_self").data)
add_as_sub_accounts = str(configs.get("add_as_sub_accounts").data)
only_unmapped_accounts = str(configs.get("only_unmapped_accounts").data)
only_consignee_mapping = str(configs.get("only_consignee_mapping").data)
only_inactive_consignees = str(configs.get("only_inactive_consignees").data)
inactive_type = str(configs.get("inactive_type").data)
country_code = str(configs.get("country_code").data)
jnj_okta = str(configs.get("jnj_okta").data)
which_location = str(configs.get("whichlocation").data)

if jnj_okta == 'yes':
    if not (which_location == 'remote' or which_location == 'remotestage'):
        exit('if using jnj okta the location of db must be remote')
elif jnj_okta == 'no':
    if not (which_location == 'local'):
        exit('if using qa okta the location of db must be local')
else:
    exit('jnj_okta parameter should be either yes or no')

if no_accounts_per_consignee > number_of_accounts / 2:
    exit("The 'no_accounts_per_consignee' setting in input.properties file should not be greater than "
         "'number_of_accounts_to_load/2'")

if number_of_accounts < 5:
    exit("The 'number_of_accounts_to_load' setting in input.properties file should be greater than 10")

if delete_existing_data == "yes":
    delete_data_account_sub_consignee()
'''
checks account name in db and if not exists then inserts to accounts table.
'''
if populate_accounts_and_mapping == "yes":
    if only_consignee_mapping == 'no':
        count = 0
        created_accounts_list = []
        while count != int(number_of_accounts):
            a = insert_one_record_accounts_table()
            if a != "":
                count = count + 1
                created_accounts_list.insert(0, a)
                print(count)

        if only_unmapped_accounts == 'yes':
            exit('')

        created_accounts_tuple = tuple(created_accounts_list)
        total_number_created = len(created_accounts_tuple)
        number_main = int(total_number_created / 10)
        number_sub = int(((total_number_created - number_main) * 50) / 100)
        number_no_mapping = total_number_created - number_sub - number_main
        main_accounts_tuple = created_accounts_tuple[0:number_main]
        sub_accounts_tuple = created_accounts_tuple[number_main:number_main + number_sub]
        no_map_accounts_tuple = created_accounts_tuple[number_main + number_sub:]
        # print("Total: "+str(total_number_created))
        # print("Main: "+str(number_main))
        # print("Sub: "+str(number_sub))
        # print("Free: "+str(number_no_mapping))
        # print("created")
        # print(created_accounts_tuple)
        # print("main")
        # print(main_accounts_tuple)
        # print("sub")
        # print(sub_accounts_tuple)
        # print("free")
        # print(no_map_accounts_tuple)
        if main_sub_acc_self == 'yes':
            # for x in created_accounts_tuple:
            #     insert_main_accounts_mapping(x)

            for i in range(len(created_accounts_tuple)):
                insert_main_accounts_mapping(created_accounts_tuple[i])
                print(str(i) + " inserting main account self mapping step1")

            # for x in created_accounts_tuple:
            #     insert_main_accounts_with_self_mapping(x)

            for i in range(len(created_accounts_tuple)):
                insert_main_accounts_with_self_mapping(created_accounts_tuple[i])
                print(str(i) + " inserting main account self mapping step2")
        elif add_as_sub_accounts == 'yes':
            # for x in created_accounts_tuple:
            #     insert_sub_accounts_mapping(x)
            for i in range(len(created_accounts_tuple)):
                insert_sub_accounts_mapping(created_accounts_tuple[i])
                print(str(i) + " inserting sub account")
        else:
            for x in main_accounts_tuple:
                insert_main_accounts_mapping(x)

            for x in sub_accounts_tuple:
                insert_sub_accounts_mapping(x)
else:
    exit("The 'populate_accounts_and_mapping' setting in input.properties file is not set to yes")

if populate_account_consignee_mapping == "yes":
    # consignee_details = get_consignee_id_list(200)
    if only_inactive_consignees == "no":
        print("---------------------------------------")
        if jnj_okta == 'yes':
            consignee_details = get_consignee_id_list_by_fn_search_jnj(consignee_fn_starts_with)
        else:
            consignee_details = get_consignee_id_list_by_fn_search_qa(consignee_fn_starts_with)

        if len(consignee_details) == 0:
            exit("No consignees found for given search criteria")
        if len(consignee_details) < no_of_consignees_to_map:
            exit("Number of consignees found are less. Change search criteria")

        no_of_consignees_mapped = 0
        for i in consignee_details:
            no_of_consignees_mapped = no_of_consignees_mapped + 1
            print('-----------------------')
            insert_consignee_account_mapping_table(to_map_accounts(no_accounts_per_consignee), i)
            insert_consignee_account_mapping_table(to_map_accounts(no_accounts_per_consignee),
                                                   {"id": "", "firstName": "Vinay Babu",
                                                    "lastName": "Gutta", "email": "VBabuGut@its.jnj.com", "phone": "+19909990912"})
            # insert_one_consignee_to_one_account_mapping_table(sub_accounts_list[rand_acc], i)
            if no_of_consignees_mapped == no_of_consignees_to_map:
                break

    if only_inactive_consignees == "yes":
        # insert_ten_consignee_per_account(get_all_sub_accounts())
        # For local okta
        # insert_ten_nonactive_consignee_per_account(get_all_sub_accounts_latest(number_of_accounts), inactive_type)
        # For Stage okta
        insert_ten_nonactive_consignee_per_account(get_all_sub_accounts_latest(number_of_accounts), country_code)

        # insert_consignee_account_mapping_table(to_map_accounts(no_accounts_per_consignee), {"id": "", "firstName": "Vinay "
        #                                                                                                            "Babu",
        #                                                                                     "lastName": "Gutta",
        #                                                                                     "email": "VBabuGut@its.jnj.com", "phone": "+19909990912"})
        insert_consignee_account_mapping_table(to_map_accounts(no_accounts_per_consignee), {"id": "", "firstName": "Leonardo",
                                                                                            "lastName": "da Vinci",
                                                                                            "email": "theautomationtester@hotmail.com", "phone": "+19909990912"})
        # insert_consignee_account_mapping_table(to_map_accounts(no_accounts_per_consignee), {"id": "", "firstName": "shrikrishna",
        #                                                                                     "lastName": "shrikrishna",
        #                                                                                     "email": "shrikrishna.m@hcl.com"})

