import os
import time

DELIM = '/$$delim_$#@'

class Operation:
    """
    Any namenode operation that causes a change in the edit log
    Store
    - op_name
    - op_timestamp
    - relevant (if any)
    - relevant metadata (if any)
    """
    # -> rm, rmdir, put, mkdir
    def __init__(self, op_name, path, data=None):
        self.time_stamp =  time.time()

        if(op_name in ['rm', 'put']):
            self.target = 'file'
        else:
            self.target = 'dir'

        if(op_name in ['rm', 'rmdir']):
            self.op_type = 'remove'
        else:
            self.op_type = 'add'
        
        self.data = data
        self.path = path
    
    def to_string(self):
        data_string = f"\n{self.data}\n" if self.data != None else "\n"
        log_string = f"{self.time_stamp} {self.target} {self.op_type} {self.path}{data_string}{DELIM}\n"
        return log_string 


class LogReader:
    """
    Should be able to 
    - Read an edit log given the filepath
    - Find relevant edits (edits made after a certain timestamp)
    - Apply edits to a given namenode directory
    """

    def __init__(self, path_to_namenode):
        self.path_to_namenode = path_to_namenode
        pass

    def read_log(self, path):

        with open(path, 'r') as f:
            f_data = f.read().strip()
            # print(f_data)
        

        print(f_data.split(DELIM))
        for line in f_data.split(DELIM)[:-1]:
            line = line.strip()
            self.apply_op(line)
        

    def apply_op(self, log):
        
        log_arr = log.split('\n')
        op = log_arr[0]

        # print(op.split())
        _, target, type, path = op.split()
        path = os.path.join(self.path_to_namenode, path)

        # put command
        if type == 'add':
            if target == 'file':
                data = ('\n').join(log_arr[1:])
                with open(path, 'w') as f:
                    f.write(data)
            else:
                # print('here')
                os.mkdir(path)

        # rm
        if type == 'remove':
            if target == 'file':
                os.remove(path)
            else:
                os.rmdir(path)



class LogWriter:
    """
    Should be able to
    - Take a list of applied `operation`s as input
    - Convert them into a log file format 
    - Write to given log file
    """
    def __init__(self, path) -> None:
        self.path = path
    
    def write_logs(self, ops):
        with open(self.path, 'w') as f:
            for op in ops:
                f.write(op.to_string())
    
if __name__ == '__main__':
    # Sample Usage
    op1 = Operation('mkdir', 'test')
    op2 = Operation('put', 'test/test1.txt', 'Hello World')
    op3 = Operation('rm', 'test/test1.txt')
    op4 = Operation('rmdir', 'test')
    lw = LogWriter([op1, op2, op3, op4])
    lw.write_logs('/mnt/c/Users/supre/BD/Yet-Another-Hadoop/directories/namenodes/logs/edits.txt')

    lr = LogReader('/mnt/c/Users/supre/BD/Yet-Another-Hadoop/directories/namenodes/namenode2')
    lr.read_log('/mnt/c/Users/supre/BD/Yet-Another-Hadoop/directories/namenodes/logs/edits.txt')