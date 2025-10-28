import calendar
from datetime import datetime

def get_total_hours(month_year_str):
	try:
		# Split into month and year
		month_part = ''.join(filter(str.isalpha, month_year_str))
		year_part = ''.join(filter(str.isdigit, month_year_str))
		
		# Convert to full date
		datetime_obj = datetime.strptime(f"{month_part} {year_part}", "%B %y")
		
		# Get number of days in the month
		_, num_days = calendar.monthrange(datetime_obj.year, datetime_obj.month)
		
		# Calculate hours
		total_hours = num_days * 24
		return total_hours
	except ValueError:
		raise Exception("Invalid input format. Use full month name followed by 2-digit year, e.g., 'January25'.")

# To find total tainted nodes hours in settings modify   cpu node_cnt to 1, gpu node_cnt to 11
# To find total untainted nodes hours in settings modify cpu node_cnt to 5, gpu node_cnt to 6
node_infos = {
	"rci-tide-cpu": {
		"cpu": 64,
		"gpu": 4,
		"node_cnt": 6
	},
	"rci-tide-gpu": { # Targets L40 GPUs
		"cpu": 24,
		"gpu": 4,
		"node_cnt": 17
	},
	"rci-nrp-gpu": { # Targets A100 GPUs
		"cpu": 64,
		"gpu": 4,
		"node_cnt": 8
	}
}

def run(monthyear, type):
	total_hours = get_total_hours(monthyear)
	if(type=="cpu"):

		cpu_info = node_infos["rci-tide-cpu"]
		cpu_cpus_avail = cpu_info["node_cnt"] * (cpu_info["cpu"]-2)

		tide_gpu_info = node_infos["rci-tide-gpu"]
		gpu_cpus_avail = tide_gpu_info["node_cnt"] * (tide_gpu_info["cpu"]-2)

		cpus_avail = cpu_cpus_avail + gpu_cpus_avail

		print(cpus_avail * total_hours)

	elif(type=="gpu"):

		tide_gpu_info = node_infos["rci-tide-gpu"]
		gpus_avail = tide_gpu_info["node_cnt"] * tide_gpu_info["gpu"]
		
		print(gpus_avail * total_hours)

for type in ["cpu", "gpu"]:
	print(f"Type: {type}")
	for monthyear in ["January25", "February25", "March25", "April25", "May25", "June25"]:
		run(monthyear, type)