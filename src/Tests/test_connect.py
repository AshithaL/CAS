from unittest import TestCase
import mock

class Test(TestCase):

    @mock.patch('src.connect.fetch_covid_state_data')
    def test_fetch_covid_state_data(self, fetch_covid_state_data):
        fetch_covid_state_data.return_value = "some.csv"
        self.assertEqual(fetch_covid_state_data(), "some.csv")

    @mock.patch('src.connect.fetch_covid_state_data')
    def test_fetch_covid_data_timeout(self, fetch_covid_state_data):
        fetch_covid_state_data.return_value = "Connection time out"
        self.assertEqual(fetch_covid_state_data(), "Connection time out")

    @mock.patch('src.connect.fetch_covid_state_data')
    def test_fetch_covid_data_redirects(self, fetch_covid_state_data):
        fetch_covid_state_data.return_value = "Too many redirects"
        self.assertEqual(fetch_covid_state_data(), "Too many redirects")

    @mock.patch('src.connect.fetch_covid_state_data')
    def test_fetch_covid_data_critical_error(self, fetch_covid_state_data):
        fetch_covid_state_data.return_value = "Critical error"
        self.assertEqual(fetch_covid_state_data(), "Critical error")

    @mock.patch('src.connect.hdfs_job')
    def test_hdfs_job(self, hdfs_job):
        hdfs_job.return_value = "Successfuly dump data in hdfs."
        self.assertEqual(hdfs_job(), "Successfuly dump data in hdfs.")

    @mock.patch('src.connect.hdfs_job')
    def test_hdfs_job_time_out(self, hdfs_job):
        hdfs_job.return_value = "Connection time out."
        self.assertEqual(hdfs_job(), "Connection time out.")

    @mock.patch('src.connect.hdfs_job')
    def test_hdfs_job_redirects(self, hdfs_job):
        hdfs_job.return_value = "Too many redirects."
        self.assertEqual(hdfs_job(), "Too many redirects.")

    @mock.patch('src.connect.hdfs_job')
    def test_hdfs_job_critical(self, sqoop_job):
        sqoop_job.return_value = "Critical error occurred during request"
        self.assertEqual(sqoop_job(), "Critical error occurred during request.")

    @mock.patch('src.connect.sqoop_job')
    def test_hdfs_job_critical(self, sqoop_job):
        sqoop_job.return_value = "sucessfully dumped in mysql"
        self.assertEqual(sqoop_job(), "sucessfully dumped in mysql")

    @mock.patch('src.connect.sqoop_job')
    def test_hdfs_job_timeout(self, sqoop_job):
        sqoop_job.return_value = "Connection time out"
        self.assertEqual(sqoop_job(), "Connection time out")

    @mock.patch('src.connect.hdfs_job')
    def test_hdfs_job_redirects(self, sqoop_job):
        sqoop_job.return_value = "Too many redirects"
        self.assertEqual(sqoop_job(), "Too many redirects")

    @mock.patch('src.connect.hdfs_job')
    def test_hdfs_job_critical(self, sqoop_job):
        sqoop_job.return_value = "Critical error"
        self.assertEqual(sqoop_job(), "Critical error")


