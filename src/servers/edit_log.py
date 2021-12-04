class Operation:
    """
    Any namenode operation that causes a change in the edit log
    Store
    - op_name
    - op_timestamp
    - relevant (if any)
    - relevant metadata (if any)
    """
    pass

class LogReader:
    """
    Should be able to 
    - Read an edit log given the filepath
    - Find relevant edits (edits made after a certain timestamp)
    - Apply edits to a given namenode directory
    """
    pass

class LogWriter:
    """
    Should be able to
    - Take a list of applied `operation`s as input
    - Convert them into a log file format 
    - Write to given log file
    """
    pass
