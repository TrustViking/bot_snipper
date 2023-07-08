#!/usr/bin/env python3
#
import subprocess
import os
import sys
import psutil
import pynvml

def memory():
    print (f'****************************************************************')
    print (f'*Data RAM {os.path.basename(sys.argv[0])}: [{psutil.virtual_memory()[2]}%]')
    # получение количества доступных устройств
    pynvml.nvmlInit()
    deviceCount = pynvml.nvmlDeviceGetCount()
    #print (f'\ndeviceCount [{deviceCount}]')
    # для каждого устройства вывод информации о загрузке памяти
    for i in range(deviceCount):
        handle = pynvml.nvmlDeviceGetHandleByIndex(i)
        #print (f'\nhandle [{handle}]')
        meminfo = pynvml.nvmlDeviceGetMemoryInfo(handle)
        #print (f'meminfo [{meminfo}]')
        print(f"#GPU [{i}]: used memory [{int(meminfo.used / meminfo.total * 100)}%]")
        print (f'****************************************************************\n')
    #return

#
def main():
    #
    print (f'\n==============================================================================\n')
    #print (f'sys.argv[0] [{sys.argv[0]}]') # [./subprocess_tf.py]
    print (f'File: [{os.path.basename(sys.argv[0])}]')
    #print (f'\n__file__ [{__file__}]') # [/media/ara/Disk1Tb_Linux/tf_linux_net/./subprocess_tf.py]
    #print (f'\nsys.path [{sys.path}]')
    print (f'Path: [{sys.path[0]}]') # [/media/ara/Disk1Tb_Linux/tf_linux_net]
    #print (f'\n==============================================================================\n')
    #
    # получение количества доступных устройств
    #pynvml.nvmlInit()
    #deviceCount = pynvml.nvmlDeviceGetCount()
    #print (f'\ndeviceCount [{deviceCount}]')
    # для каждого устройства вывод информации о загрузке памяти
    #for i in range(deviceCount):
    #    handle = pynvml.nvmlDeviceGetHandleByIndex(i)
    #    print (f'\nhandle [{handle}]')
    #    meminfo = pynvml.nvmlDeviceGetMemoryInfo(handle)
    #    print (f'meminfo [{meminfo}]')
    #    print(f"GPU [{i}]: used memory [{int(meminfo.used / meminfo.total * 100)}%]")
    #    
    # список скриптов для выполнения
    path_scripts1=os.path.join(sys.path[0], 'bot_telega_v1.py')
    #path_scripts2=os.path.join(sys.path[0], 'bot_telega_v1.py')
    #path_scripts3=os.path.join(sys.path[0], 'krsChar_blstm_2v3.py')
    #path_scripts4=os.path.join(sys.path[0], 'krsChar_1v4.py')
    #path_scripts5=os.path.join(sys.path[0], 'krsChar_1v1.py')
    #path_scripts6=os.path.join(sys.path[0], 'krsChar_blstm_2v3.py')
    #path_scripts7=os.path.join(sys.path[0], 'Char_bider_lstm_2v3.py')
    #
    scripts = [
               path_scripts1, 
               #path_scripts2, 
               #path_scripts3, 
               #path_scripts4, 
               #path_scripts5, 
               #path_scripts6, 
               #path_scripts7, 
               ] 
    #
    #path_scriptsOne=os.path.join(sys.path[0], 'modKerasChar_4v1_tuner.py')
    #path_scripts1=os.path.join(sys.path[0], 'modKerasChar_4_m.py')
    #scripts = [path_scriptsOne]
    #scripts = [path_scripts1, path_scripts2]
    #
    memory()
    #
    for script in scripts:
        # запуск скрипта
        #print (f'\n==============================================================================\n')
        print (f'Execution script: [{script}]')
        #print (f'Data memory: [{psutil.virtual_memory()}]')
        #mem = psutil.virtual_memory()
        process = subprocess.Popen(['python3', script])
        #print (f'\nData memory after make process: \n[{psutil.virtual_memory()}]')
        #print (f'\n==============================================================================\n')
        #print (f'\nEnd process: [{process}]')

        # Взаимодействовать с процессом: 
        # отправить данные на стандартный ввод. 
        # Чтение данных из stdout и stderr до тех пор, пока не будет достигнут конец файла.
        #print (f'\n==============================================================================\n')
        #print (f'memory cleaning')
        process.communicate()
        #print (f'\nresult process_communicate: [{process_communicate}]')
        #print (f'\nData memory after communicate process: [{psutil.virtual_memory()}]')
        #print (f'\n==============================================================================\n')
        #
        # очистка оперативной памяти
        print (f'==============================================================================')
        memory()
        print (f'delete process...')
        del process
        memory()
        #print (f'\nData memory after delete process: [{psutil.virtual_memory()}]')
        # для каждого устройства вывод информации о загрузке памяти
        #for i in range(deviceCount):
            #handle = pynvml.nvmlDeviceGetHandleByIndex(i)
            #print (f'\nhandle [{handle}]')
            #meminfo = pynvml.nvmlDeviceGetMemoryInfo(handle)
            #print (f'meminfo [{meminfo}]')
            #print(f"GPU [{i}]: used memory [{int(meminfo.used / meminfo.total * 100)}%]")
        #
    # завершение всех процессов Python
    print (f'==============================================================================')
    print (f'\nkill all process')
    subprocess.call(['killall', 'python3'])
    #print (f'Data memory: [{psutil.virtual_memory()}]')
    #memory()

if __name__ == "__main__":
    main()
