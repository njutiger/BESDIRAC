#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: linlei


#from optparse import OptionParser
from argparse import ArgumentParser 
import os
import os.path
import time

class SplitJob:
    def __init__(self,dirs_txt,filestep,jobnum,template):
        self.dirs_txt=dirs_txt
        self.filestep=filestep
        self.jobnum=jobnum
        self.template=template

    #use getAllFiles to get all files in these directories 
    def _getAllFiles(self):
        dirs = []
        linenum = 0

        if os.path.exists(self.dirs_txt):
            f = open(self.dirs_txt,"r")
            allLines = f.readlines()
            f.close()

            for eachLine in allLines:
                dir = eachLine.strip()
                dirs.append(dir)
        else:
            print "Your file %s is not exit"%dstdir

        files_txt = self.dirs_txt + ".files"
        fp = open(self.dirs_txt,"w")
        for d in sorted(dirs):
            if os.path.isabs(d):
                files = os.listdir(d)
                for f in sorted(files):
                    #only dst file can be written into the txt file 
                    if f.endswith(".dst"):
                        linenum += 1
                        fulldir = os.path.join(d,f)
                        fp.write(fulldir+'\n')
                    else:
                        fulldir = os.path.join(d,f)
                        print "%s is not dst file"%fulldir
                        continue
                        
        fp.close()

        return linenum,files_txt

    #creat script which we can use to submit job 
    def _createSubScript(self,files_txt,linefrm,lineto,subjob):
        currentdir = os.getcwd()
        
        rootfile = files_txt + str(subjob) + ".root"

        #the most important instruction,use this can execute control.py 
        #to get attributes and upload this file to DFC 
        executestr = "python "+currentdir+"/control.py -f "+str(linefrm)+" -o "+str(lineto)+" -d "+files_txt+" -r "+rootfile

        #set name of this subJob script
        subjob = files_txt + ".job" + str(subjob)
        #copy content of Template to this subjob script
        if os.path.exists(self.template):
            filefrm = open(self.template,"r")
            fileto = open(subjob,"w")
            for eachline in filefrm:
                fileto.write(eachline)
            #write python instruction to subjob script
            fileto.write(executestr + "\n")

            filefrm.close()
            fileto.close()
        else:
            print"%s has not found"%template
            
    #split job according filestep
    def _splitJob_file(self):
        linefrm = 0
        lineto  = 0
        num = 0

        linenum,files_txt = self._getAllFiles()
        
        for i in range(0,linenum,self.filestep):
            linefrm = i
            lineto  = i + self.filestep

            if lineto > linenum:
                lineto = linenum
               
            self._createSubScript(files_txt,linefrm,lineto,num)
            num += 1
            
    #split job according jobnum
    def _splitJob_job(self):
        linefrm = 0
        lineto = 0

        linenum,files_txt = self._getAllFiles()
        list1 = list(divmod(linenum,self.jobnum))
        
        filestep = list1[0]
        remainder = list1[1]

        for eachjob in range(self.jobnum):
            if eachjob != self.jobnum-1:
                linefrm = filestep * eachjob
                lineto = filestep * (eachjob + 1)
                
            else:
                linefrm = filestep * eachjob
                lineto = filestep * (eachjob + 1) + remainder

            self._createSubScript(files_txt,linefrm,lineto,eachjob)

    def split(self):
        #split according number of files in every subjob
        if self.filestep:
            self._splitJob_file()
        #split according number of subjobs
        elif self.jobnum:
            self._splitJob_job()

if __name__ =="__main__":

    parser =ArgumentParser()
    parser.add_argument('--dirs',dest='dirs', type=str, required=True, help='contains direcotries of files')
    parser.add_argument('--template',dest='template', type=str, required=True, help='template for submitting subjobs')
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--filenum',dest='filenum', type=int, help='how many files of a small job')
    group.add_argument('--jobnum',dest='jobnum', type=int, help='how many subjobs')

    args = parser.parse_args()
    
    dirs = args.dirs
    template = args.template
    filenum = args.filenum
    jobnum = args.jobnum
    
    split = SplitJob(dirs,filenum,jobnum,template) 
    split.split()
