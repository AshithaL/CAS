import datetime
import subprocess


def run_cmd(args_list):
    """
    run linux commands
    """
    # import subprocess
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err

date = datetime.datetime.today().strftime('%Y-%m-%d')
# Run Hadoop copyFromLocal command in Python
(ret, out, err) = run_cmd(
['hdfs', 'dfs', '-copyFromLocal', '/home/nineleaps/PycharmProjects/CAS/src/Data/data{}.csv'.format(date),
'/user/covid_data_2020-06-26.csv'])
print(ret, out, err)

