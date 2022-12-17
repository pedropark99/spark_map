import os
import platform

os_name = platform.system()

if os_name == 'Windows':
    py_command = 'py'
else:
    py_command = 'python3'

### BUILD ===============================
cmd = py_command + ' -m build'
os.system(cmd)


### CHECK IF PACKAGE IS INSTALLED =======
