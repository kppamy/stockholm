from pydata import Pydata
from grab import Grab
import option
import os

def checkFoldPermission(path):
    if(path == 'USER_HOME/tmp/pydata_export'):
        path = os.path.expanduser('~') + '/tmp/pydata_export'
    try:
        if not os.path.exists(path):
            os.makedirs(path)
        else:
            txt = open(path + os.sep + "test.txt","w")
            txt.write("test")
            txt.close()
            os.remove(path + os.sep + "test.txt")
            
    except Exception as e:
        print(e)
        return False
    return True
def main():
    args = option.parser.parse_args()
    if not checkFoldPermission(args.store_path):
        print('\nPermission denied: %s' % args.store_path)
        print('Please make sure you have the permission to save the data!\n')
    else:
        print('Stockholm is starting...\n')
        grab=Grab(args)
        grab.run()
        #dic={}
        #all_quotes=grab.read_csv_file(dic)

        ##all_quotes=df.to_dict('records')
        #stockh = Pydata(args)
        #stockh.run(all_quotes)
        #print('Stockholm is done...\n')
        #return all_quotes

if __name__ == '__main__':
    main()
