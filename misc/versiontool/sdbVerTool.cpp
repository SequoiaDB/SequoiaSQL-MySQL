/*******************************************************************************

   Copyright (C) 2011-Present SequoiaDB Ltd.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   Source File Name = sdbVerTool.cpp

   Descriptive Name = N/A

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          08/08/2025  SequoiaDB  Initial Draft

   Last Changed =

*******************************************************************************/
#include <iostream>
#include <stdio.h>
#include "ossVer.hpp"

using namespace std;

int main()
{
   int major = 0 ;
   int minor = 0 ;
   int patch = 0 ;
   int release = 0 ;
   const CHAR *buildTime = NULL ;
   const CHAR *gitVer = NULL ;
   ossGetVersion(&major, &minor, &patch, &release, &buildTime, &gitVer) ;

   char version[128] = {0};
   if (patch > 0)
   {
      sprintf(version, "Version: %d.%d.%d\nRelease: %d\nGit version: %s\n%s",
              major, minor, patch, release, gitVer, buildTime) ;
   }
   else
   {
      sprintf(version, "Version: %d.%d\nRelease: %d\nGit version: %s\n%s",
              major, minor, release, gitVer, buildTime) ;
   }
   cout << version << endl ;

   return 0;
}