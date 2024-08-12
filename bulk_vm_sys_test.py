import math
import numpy as np
import requests
from threading import *

from ergon.client.task_poller import TaskPoller
from ergon.client.client import ErgonClient
from ergon.ergon_interface_pb2 import TaskGetArg

import time
from util.base.types import NutanixUuid

import gflags
gflags.FLAGS([])

import urllib3
from collections import Counter
import logging
import sys
filename = sys.argv[1]

logging.basicConfig(
	filename=filename,
	format='%(asctime)s %(levelname)-8s %(message)s',
	level=logging.INFO,
	datefmt='%Y-%m-%d %H:%M:%S')

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


ergon_client = ErgonClient()
poller = TaskPoller("Benchmark", ergon_client)

obj = Semaphore(20)

master_vm_id = "8b92984c-80b3-4e7f-b64d-8c139ee70902"
container_uuid = "124aa356-f648-447a-8bfb-d5aaedb94cbf"
num_vms_scale = 1500

vm_get_url = "https://localhost:9440/PrismGateway/services/rest/v2.0/vms/{}?include_vm_disk_config=true&include_vm_nic_config=true"
def vm_get(vm_id):
	url = vm_get_url.format(vm_id)
	response = requests.get(url, auth = ('admin', 'Nutanix.123'), verify=False)
	# logging.info(response.json())
	return response

vm_clone_url = "https://localhost:9440/PrismGateway/services/rest/v2.0/vms/{}/clone"
def vm_clone(master_vm_id, new_vm_name, task_list):
	obj.acquire()
	url = vm_clone_url.format(master_vm_id)
	# logging.info("vm_clone called for: {}".format(new_vm_name))
	request = {"spec_list": [{"name": new_vm_name}]}
	response = requests.post(url, json=request, auth = ('admin', 'Nutanix.123'), verify=False)
	if response.status_code != 201:
		logging.error("vm_clone response for new_vm_name: {} {}".format(new_vm_name, response))
	task_uuid_str = response.json()["task_uuid"]
	task_uuid = NutanixUuid.from_hex(task_uuid_str)
	task_list.append(task_uuid.bytes)
	poller.poll_for_completion([task_uuid])
	logging.info("vm_clone task {} completed for vm {}".format(task_uuid, new_vm_name))
	obj.release()
	

vm_list_url = "https://localhost:9440/PrismGateway/services/rest/v2.0/vms/?include_vm_disk_config=true&sort_attribute=uuid&sort_order=ascending&offset=%d&length=100"
def list_vm():
	vm_uuids =[]
	for i in range(0, 3000, 100):
		logging.info("Fetching vms from offset {}".format(i))
		url = vm_list_url % i
		response = requests.get(url, auth = ('admin', 'Nutanix.123'), verify=False)
		response_json = response.json()
		if len(response_json["entities"]) == 0:
			break
		resp_vm_uuids = [vm["uuid"] for vm in response_json["entities"]]
		vm_uuids.extend(resp_vm_uuids)
	logging.info("Listed {} VMs".format(len(vm_uuids)))
	# frequency_map = Counter(vm_uuids)
	# for element, frequency in frequency_map.items():
	# 	if frequency > 1:
	# 		raise Exception("Faulty VM List, Element: {}, Frequency: {}".format(element, frequency))
	return vm_uuids


vm_disk_attach = "https://localhost:9440/PrismGateway/services/rest/v2.0/vms/{}/disks/attach"
def add_disk(vm_id, task_list):
	obj.acquire()
	vm_get_response = vm_get(vm_id)
	url = vm_disk_attach.format(vm_id)
	# logging.info("add_disk called for vm_id: {}".format(vm_id))
	request = {"uuid": vm_id, "vm_disks": [{"disk_address": {"device_bus": "SCSI", "device_index": 2}, "vm_disk_create": {"size": 10737418240, "storage_container_uuid": container_uuid}}]}
	# logging.info(request)
	response = requests.post(url, json=request, auth = ('admin', 'Nutanix.123'), verify=False)
	if response.status_code != 201:
 		logging.error("add_disk response for vm_id: {} {}".format(vm_id, response))
	task_uuid_str = response.json()["task_uuid"]
	task_uuid = NutanixUuid.from_hex(task_uuid_str)
	task_list.append(task_uuid.bytes)
	poller.poll_for_completion([task_uuid])
	logging.info("add_disk task {} completed for vm {}".format(task_uuid, vm_id))
	obj.release()


vm_delete_url = "https://localhost:9440/PrismGateway/services/rest/v2.0/vms/{}"
def vm_delete(vm_id, task_list):
	obj.acquire()
	url = vm_delete_url.format(vm_id)
	# logging.info("vm_delete called for vm_id: {}".format(vm_id))
	response = requests.delete(url, auth = ('admin', 'Nutanix.123'), verify=False)
	if response.status_code != 201:
		logging.error("vm_delete response for vm_id: {} {}".format(vm_id, response))
	task_uuid_str = response.json()["task_uuid"]
	task_uuid = NutanixUuid.from_hex(task_uuid_str)
	task_list.append(task_uuid.bytes)
	poller.poll_for_completion([task_uuid])
	logging.info("vm_delete task {} completed for vm {}".format(task_uuid, vm_id))
	obj.release()


def print_tasks_stats(task_list):
	arg = TaskGetArg(task_uuid_list=task_list, include_subtask_uuids=False)
	tasks = ergon_client.TaskGet(arg).task_list
	logging.info("\nTotal tasks created: {}".format(len(task_list)))


def main():
	base_vm_name = "sys-test"
	vm_names = [base_vm_name+"-"+str(i) for i in range(num_vms_scale)]

	step_name = "Clone VM"
	logging.info("\nExecuting {} for {} iterations".format(step_name, len(vm_names)))
	task_list = []
	threads = [Thread(target=vm_clone, args=(master_vm_id, name, task_list)) for name in vm_names]
	start_time = time.time()
	[thread.start() for thread in threads]
	[thread.join() for thread in threads]
	end_time = time.time()
	logging.info("Time taken for {}: {} seconds".format(step_name, end_time-start_time))
	print_tasks_stats(task_list)

	step_name = "List VMs"
	logging.info("\nExecuting {}".format(step_name))
	start_time = time.time()
	vm_uuids = list_vm()
	end_time = time.time()
	logging.info("Time taken for {}: {} seconds".format(step_name, end_time-start_time))

	try:
		vm_uuids.remove(master_vm_id)
	except ValueError:
		pass

	step_name = "Attach Disk to VM"
	logging.info("\nExecuting {} for {} iterations".format(step_name, len(vm_uuids)))
	task_list = []
	threads = [Thread(target=add_disk, args=(vm_id, task_list)) for vm_id in vm_uuids]
	start_time = time.time()
	[thread.start() for thread in threads]
	[thread.join() for thread in threads]
	end_time = time.time()
	logging.info("Time taken for {}: {} seconds".format(step_name, end_time-start_time))
	print_tasks_stats(task_list)
    
	step_name = "Delete VM"
	logging.info("\nExecuting {} for {} iterations".format(step_name, len(vm_uuids)))
	task_list = []
	threads = [Thread(target=vm_delete, args=(vm_id, task_list)) for vm_id in vm_uuids]
	start_time = time.time()
	[thread.start() for thread in threads]
	[thread.join() for thread in threads]
	end_time = time.time()
	logging.info("Time taken for {}: {} seconds".format(step_name, end_time-start_time))
	print_tasks_stats(task_list)
    
    
if __name__=="__main__":
	script_start_time = time.time()
	main()
	logging.info("Total script execution time: {} seconds".format(time.time()-script_start_time))
 