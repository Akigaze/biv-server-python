import os
import re
import time
from collections import namedtuple
from datetime import datetime
from random import random

import xlrd

from util.httputil import form_request, get_request
from util.listutil import get_sub_list

Resource = namedtuple("Resource", ["value", "uri"])

COMPLETE = 7
WAIT = 0
START = 1
JOB_NOT_COMPLETE_SYMBOL = "<h2>Phyre2 File not Found</h2>"
PDB_DIR_PATH = "E://PythonStation//project//bio-info-visvility//biv-server//output//phyre2//pdb//"
FINIAL_COMPLETE_FILE_PATH = "E://PythonStation//project//bio-info-visvility//biv-server//output//phyre2//complete.txt"


class Phyre2Service(object):
    HOME = "http://www.sbg.bio.ic.ac.uk/phyre2/webscripts/phyre2_submit.cgi"
    OUTPUT_PAGE_BASE = "http://www.sbg.bio.ic.ac.uk/phyre2/phyre2_output/{jobid}/summary.html"
    REDIRECT_BASE = "http://www.sbg.bio.ic.ac.uk/phyre2/webscripts/jobmonitor.cgi?jobid={jobid}"
    RESULT_BASE = "http://www.sbg.bio.ic.ac.uk/phyre2/phyre2_output/{jobid}/summary.html"
    PDB_BASE = "http://www.sbg.bio.ic.ac.uk/phyre2/phyre2_output/{jobid}/final.casp.pdb"

    def __init__(self):
        self.email = "akigaze.hwang@outlook.com"
        self.model_mode = "normal"
        self.user_type = "NA"
        self.btns = "Phyre Search"
        self.template_regex = re.compile(r"\<a href=\"http:\/\/www.sbg.bio.ic.ac.uk\/phyre2\/html\/flibview.cgi\?pdb=[\w\d]+?\"[\w\W]*?>([\w\d]+?)<\/a>")

        self.resource = []
        pass

    def batch_post(self, prefix, data=None, file=None, filepath=None):
        if filepath is None:
            return False
        excel = xlrd.open_workbook(filepath)
        sheet = excel.sheet_by_index(0)
        print("get table: ", sheet.nrows, sheet.ncols)
        rows = sheet.get_rows()
        for row in rows:
            description, sequence = row[0].value, row[1].value
            print(description, sequence)
            form = self._build_form_data(prefix+description, sequence)
            status, content = self._do_submit(form)
            print(status)
            print(content)
            job_id = None
            if status == 200:
                job_id = self._find_job_id(content)
            print("job id: ", job_id)
            self.resource.append(JobResource(description, sequence, status, job_id))
            time.sleep(random() * 5)
        self.serialize_resource()
        print("serialize jobs: ", len(self.resource))

    def _build_form_data(self, description, sequence):
        return {
            "usr-email": self.email,
            "seq-desc": description,
            "sequence": sequence,
            "modelmode": self.model_mode,
            "usertype": self.user_type,
            "btnS": self.btns
        }

    def _do_submit(self, form):
        response = form_request(Phyre2Service.HOME, form)
        return response.status_code, response.text

    def _do_get_result(self, job_id):
        url = Phyre2Service.RESULT_BASE.format(jobid=job_id)
        response = get_request(url)
        return response.status_code, response.text

    def _find_job_id(self, content):
        pattern = r"(http:\/\/www.sbg.bio.ic.ac.uk\/phyre2\/webscripts\/jobmonitor.cgi)\?jobid=([\w\d]+)"
        result = re.search(pattern, content)
        if len(result.groups()) == 2:
            return result.group(2)
        return None

    def serialize_resource(self):
        header = "jobId,description,sequence,statusCode,postTime"
        now_str = datetime.now().strftime("%Y%m%d%H%M%S")
        output_dir = "E://PythonStation//project//bio-info-visvility//biv-server//output//phyre2//"
        output_file_path = output_dir + now_str + ".txt"
        lines = [header+os.linesep]
        lines.extend([str(job)+os.linesep for job in self.resource])
        with open(output_file_path, "x") as file:
            file.writelines(lines)

    def check_and_retrieve_job_result(self):
        active_jobs = [job for job in self.resource if job.status != COMPLETE]
        while len(active_jobs) > 0:
            for job in active_jobs:
                status_code, content = self._do_get_result(job.job_id)
                print(job.job_id, "status: ", status_code)
                if status_code != 200 or JOB_NOT_COMPLETE_SYMBOL in content:
                    continue
                t1, t2, t3 = self.retrieve_templates(content)
                print("$$$$$$$ get templates complete: ", t1, t2, t3)
                if None in [t1, t2, t3]:
                    continue
                model = self.retrieve_protein_model_pdb(job.job_id, job.description)
                print("^^^^^^^ retrieve model complete: ", model)
                job.job_result = JobResult(model, t1, t2, t3)
                job.status = COMPLETE
            self._save_complete_jobs(active_jobs)
            active_jobs = [job for job in active_jobs if job.status != COMPLETE]
            print("****** rest jobs: ", len(active_jobs))
            time.sleep(60 * 5)

    def _save_complete_jobs(self, jobs):
        complete_jobs = [job for job in jobs if job.status == COMPLETE]
        if len(complete_jobs) > 0:
            with open(FINIAL_COMPLETE_FILE_PATH, "a") as file:
                lines = [str(job) + os.linesep for job in complete_jobs]
                file.writelines(lines)
        print("----- save complete jobs: ", len(complete_jobs))

    def retrieve_protein_model_pdb(self, job_id, description):
        url = Phyre2Service.PDB_BASE.format(jobid=job_id)
        response = get_request(url)
        status, content = response.status_code, response.content
        file_name, path = self._get_pdb_file_path(description)
        with open(path, "wb") as file:
            file.write(content)
        return file_name

    def _get_pdb_file_path(self, name):
        file_name = name + ".pdb"
        path = PDB_DIR_PATH + file_name
        return file_name, path

    def retrieve_templates(self, text):
        templates = self.template_regex.findall(text)
        return tuple(get_sub_list(templates, length=3))



class JobResource(object):
    def __init__(self, description, sequence, status_code, job_id):
        self.description = description
        self.sequence = sequence
        self.status_code = int(status_code)
        self.job_id = job_id
        self.job_result = None
        self.post_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.status = WAIT

    def __str__(self):
        result = "no yet"
        if self.job_result:
            result = str(self.job_result)
        return "%s,%s,%s,%d,%s,%s,%s" % (
            self.job_id, self.description, self.sequence, self.status_code, self.post_time, self.status, result)


class JobResult(object):

    def __init__(self, model, t1, t2, t3):
        self.protein_model = model
        self.template_1 = t1
        self.template_2 = t2
        self.template_3 = t3

    def __str__(self):
        return "%s,%s,%s,%s" % (self.protein_model, self.template_1, self.template_2, self.template_3)


def check():
    service = Phyre2Service()
    regex = re.compile(r"[,\n]")
    complete_job_ids = []
    with open("E://PythonStation//project//bio-info-visvility//biv-server//output//phyre2//complete.txt",
              "r") as stream:
        lines = stream.readlines()
        valid_line = [line for line in lines[1:] if not regex.match(line)]
        for line in valid_line:
            job_id = line.replace("\n", "").split(",")[0]
            complete_job_ids.append(job_id)
    with open("E://PythonStation//project//bio-info-visvility//biv-server//output//phyre2//20190706234817.txt",
              "r") as stream:
        lines = stream.readlines()
        valid_line = [line for line in lines[1:] if not regex.match(line)]
        for line in valid_line:
            job_id, desc, seq, status_code, post_time, status_code, result = tuple(line.replace("\n", "").split(","))
            if job_id in complete_job_ids:
                continue
            job = JobResource(desc, seq, status_code, job_id)
            job.post_time = post_time
            service.resource.append(job)
    print("service complete: ", len(service.resource))
    service.check_and_retrieve_job_result()


def request():
    service = Phyre2Service()
    path = "E://PythonStation//project//bio-info-visvility//test-files//phyre2//eucalypt-proteins-16.xlsx"
    service.batch_post("4 - ", filepath=path)


if __name__ == "__main__":
    check()
    # request()
