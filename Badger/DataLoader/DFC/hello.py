

import DIRAC

from DIRAC.Core.Base import Script

Script.parseCommandLine()

from DIRAC import gConfig
print dir(gConfig)
print gConfig.getOptions("/Hello", "")
print gConfig.getOption("/Hello/World", "")
print gConfig.getOptionsDict("/Hello") 
