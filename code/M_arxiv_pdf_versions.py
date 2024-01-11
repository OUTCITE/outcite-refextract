import sys, os


_infolder = sys.argv[1];

filenames = os.listdir(_infolder);

names = dict()
for filename in filenames:
    try:
        name, version = filename[:-4].split('v');
    except:
        print('Error:',filename);
        name, version = filename, '1';
    if name in names:
        names[name].append(version);
    else:
        names[name] = [version]

print(len(filenames),len(names),round(100*len(names)/len(filenames)));

for name in names:
    latest = sorted(names[name])[-1];
    keep   = name+'v'+latest+'.pdf'
    remove = [name+'v'+version+'.pdf' for version in names[name] if not name+'v'+version+'.pdf'==keep];
    for remove_file in remove:
        if os.path.exists(remove_file):
            os.remove(_infolder+remove_file)
        else:
            print('ERROR:',_infolder+remove_file,"does not exist!")
    #print(name,names[name],latest,'keep:',keep,'remove:',remove);
