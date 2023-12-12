import os
import sys

import requests


def get_consignee_id_list(number_required):
    if (not (number_required % 200) == 0) and number_required > 200:
        exit("only multiples of 200 allowed to retrieve consignees")
    consignee_ids_list = []
    try:
        consignee_count = 0
        iteration_count = 0
        while consignee_count < number_required:
            if number_required > 200:
                if iteration_count == 0:
                    headers_for = {"Authorization": "SSWS00EkrobpV51gXD_y0wYUAelSpj_EWIBipEbS8hcGk3",
                                   "Content-Type": "application/json",
                                   "Accept": "application/json"}
                    url = 'https://dev-67659300.okta.com/api/v1/users?filter=status eq "ACTIVE"&limit=200'

                    response = requests.get(url, headers=headers_for, verify=False)
                    # print(type(response.json()))
                    first_list = response.json()
                    # print(first_list[0]['id'])
                    # print(first_list)
                    for i in first_list:
                        c_data = {"id": i['id'], "firstName": i['profile']['firstName'],
                                  "lastName": i['profile']['lastName'], "email": i['profile']['email']}
                        consignee_ids_list.append(c_data)
                    consignee_count = int(len(consignee_ids_list))
                    iteration_count = iteration_count + 1
                else:
                    headers_for = {"Authorization": "SSWS00EkrobpV51gXD_y0wYUAelSpj_EWIBipEbS8hcGk3",
                                   "Content-Type": "application/json",
                                   "Accept": "application/json"}
                    url_next = 'https://dev-67659300.okta.com/api/v1/users?after=' \
                               + str(consignee_ids_list[-1]["id"]) + \
                               '&limit=200&filter' \
                               '=status ' \
                               'eq "ACTIVE"'
                    response = requests.get(url_next, headers=headers_for, verify=False)
                    print(type(response.json()))
                    first_list = response.json()
                    # print(first_list[0]['id'])
                    # print(first_list)
                    # print(response.headers)
                    for i in first_list:
                        c_data = {"id": i['id'], "firstName": i['profile']['firstName'],
                                  "lastName": i['profile']['lastName'], "email": i['profile']['email']}
                        consignee_ids_list.append(c_data)
                    consignee_count = int(len(consignee_ids_list))

            else:
                headers_for = {"Authorization": "SSWS00EkrobpV51gXD_y0wYUAelSpj_EWIBipEbS8hcGk3",
                               "Content-Type": "application/json",
                               "Accept": "application/json"}
                url = 'https://dev-67659300.okta.com/api/v1/users?filter=status eq "ACTIVE"&limit=' + str(
                    number_required)

                response = requests.get(url, headers=headers_for, verify=False)
                # print(type(response.json()))
                first_list = response.json()
                # print(first_list[0]['id'])
                # print(first_list)
                for i in first_list:
                    c_data = {"id": i['id'], "firstName": i['profile']['firstName'],
                              "lastName": i['profile']['lastName'], "email": i['profile']['email']}
                    consignee_ids_list.append(c_data)
                consignee_count = int(len(consignee_ids_list))
        return consignee_ids_list
    except Exception:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, file_name, exc_tb.tb_lineno)


def get_consignee_id_list_by_fn_search_jnj(search_string):
    consignee_ids_list = []
    try:
        # 00EkrobpV51gXD_y0wYUAelSpj_EWIBipEbS8hcGk3
        # JnJ
        # 00LtZq9DkNIoLeUh9U-UWy1EkBx4cdklo_G1imIf4S
        headers_for = {"Authorization": "SSWS00LtZq9DkNIoLeUh9U-UWy1EkBx4cdklo_G1imIf4S",
                       "Content-Type": "application/json",
                       "Accept": "application/json"}
        # url = 'https://dev-67659300.okta.com/api/v1/users?search=profile.firstName sw "' + str(search_string) + '"'
        url = 'https://dev-jnj-hmt.oktapreview.com/api/v1/users?search=profile.firstName sw "' + str(
            search_string) + '"'

        response = requests.get(url, headers=headers_for, verify=False)
        # print(type(response.json()))
        first_list = response.json()
        # print(first_list[0]['id'])
        # print(url)
        print(first_list)
        for i in first_list:
            c_data = {"id": i['id'], "firstName": i['profile']['firstName'],
                      "lastName": i['profile']['lastName'], "email": i['profile']['email']}
            consignee_ids_list.append(c_data)

        return consignee_ids_list
    except Exception:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, file_name, exc_tb.tb_lineno)


def get_consignee_id_list_by_fn_search_qa(search_string):
    consignee_ids_list = []
    try:
        # 00EkrobpV51gXD_y0wYUAelSpj_EWIBipEbS8hcGk3
        # JnJ
        # 00LtZq9DkNIoLeUh9U-UWy1EkBx4cdklo_G1imIf4S
        headers_for = {"Authorization": "SSWS00EkrobpV51gXD_y0wYUAelSpj_EWIBipEbS8hcGk3",
                       "Content-Type": "application/json",
                       "Accept": "application/json"}
        url = 'https://dev-67659300.okta.com/api/v1/users?search=profile.firstName sw "' + str(search_string) + '"'

        response = requests.get(url, headers=headers_for, verify=False)
        # print(type(response.json()))
        first_list = response.json()
        # print(first_list[0]['id'])
        # print(url)
        print(first_list)
        for i in first_list:
            c_data = {"id": i['id'], "firstName": i['profile']['firstName'],
                      "lastName": i['profile']['lastName'], "email": i['profile']['email'], "phone": i['profile']['primaryPhone']}
            consignee_ids_list.append(c_data)

        return consignee_ids_list
    except Exception:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, file_name, exc_tb.tb_lineno)
