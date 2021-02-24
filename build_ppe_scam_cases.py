#!/usr/bin/env python                                                                                                   
      
import os, sys
from netCDF4 import Dataset

# This script must be run in an environment with netCDF4 python libraries 
# active and the ability to build cesm cases.
# On Cheyenne, the best way to do this is from an interactive session.
# > qsub -X -I -l select=1:ncpus=36:mpiprocs=36 -l walltime=01:00:00 -q regular -A P93300642
# > conda activate my_env
# > ./build_ppe_scam_cases.py

# Edit below to set up your cases
cesmroot = '/glade/work/katec/mg3work/PPE_PUMAS_Dev_fin'
basecasename = "PPE_KTC_test4"
baseroot = os.path.join("/glade/work/katec/mg3work","ppe_cases",basecasename)
res = "T42_T42"
compset = "FSCAM"
iop = "arm97"
user_mods_dir = os.path.join(cesmroot,"cime_config","usermods_dirs","scam_"+iop)
paramfile = "./parameter.nc"
ensemble_startval = "001" # The startval strings should be the same length, or else
basecase_startval = "000"
project = "P93300642"

if cesmroot is None:
    raise SystemExit("ERROR: CESM_ROOT must be defined in environment")
_LIBDIR = os.path.join(cesmroot,"cime","scripts","Tools")
sys.path.append(_LIBDIR)
_LIBDIR = os.path.join(cesmroot,"cime","scripts","lib")
sys.path.append(_LIBDIR)

import datetime, glob, shutil
import CIME.build as build
from standard_script_setup import *
from CIME.case             import Case
from CIME.utils            import safe_copy
from argparse              import RawTextHelpFormatter
from CIME.locked_files          import lock_file, unlock_file

def per_run_case_updates(case, user_mods_dir, ensemble_str, nint, paramdict):
    print(">>>>> BUILDING CLONE CASE...")
    caseroot = case.get_value("CASEROOT")
    basecasename = os.path.basename(caseroot)[:-nint]

    unlock_file("env_case.xml",caseroot=caseroot)
    casename = basecasename+ensemble_str
    case.set_value("CASE",casename)
    rundir = case.get_value("RUNDIR")
    rundir = rundir[:-nint]+ensemble_str
    case.set_value("RUNDIR",rundir)
    case.flush()
    lock_file("env_case.xml",caseroot=caseroot)
    print("...Casename is {}".format(casename))
    print("...Caseroot is {}".format(caseroot))
    print("...Rundir is {}".format(rundir))

    # restage user_nl files for each run                                                                                     
    for usermod in glob.iglob(user_mods_dir+"/user*"):
        safe_copy(usermod, caseroot)

    paramLines = []
    ens_idx = int(ensemble_str)-int(ensemble_startval)
    for var in paramdict.keys():
        paramLines.append("{} = {}\n".format(var,paramdict[var][ens_idx]))

    usernlfile = os.path.join(caseroot,"user_nl_cam")
    print("...Writing to user_nl file: "+usernlfile)
    file1 = open(usernlfile, "a")
    file1.writelines(paramLines)
    file1.close()

    print(">> Clone {} case_setup".format(ensemble_str))
    case.case_setup()
    print(">> Clone {} create_namelists".format(ensemble_str))
    case.create_namelists()
    print(">> Clone {} submit".format(ensemble_str))
    case.submit()


def build_base_case(baseroot, basecasename, res, compset, overwrite,
                    user_mods_dir):
    print(">>>>> BUILDING BASE CASE...")
    caseroot = os.path.join(baseroot,basecasename+'.'+basecase_startval)
    if overwrite and os.path.isdir(caseroot):
        shutil.rmtree(caseroot)
    with Case(caseroot, read_only=False) as case:
        if not os.path.isdir(caseroot):
            case.create(os.path.basename(caseroot), cesmroot, compset, res,
                        machine_name="cheyenne", driver="mct",
                        run_unsupported=True, answer="r",walltime="01:00:00",
                        user_mods_dir=user_mods_dir, project=project)
            # make sure that changing the casename will not affect these variables                                           
            case.set_value("EXEROOT",case.get_value("EXEROOT", resolved=True))
            case.set_value("RUNDIR",case.get_value("RUNDIR",resolved=True)+".00")

            case.set_value("RUN_TYPE","startup")
            case.set_value("GET_REFCASE",False)


        rundir = case.get_value("RUNDIR")
        caseroot = case.get_value("CASEROOT")
        print(">> base case_setup...")
        case.case_setup()
        print(">> base case_build...")
        build.case_build(caseroot, case=case)

        return caseroot

def clone_base_case(caseroot, ensemble, user_mods_dir, overwrite, paramdict):
    print(">>>>> CLONING BASE CASE...")
    startval = ensemble_startval
    nint = len(ensemble_startval)
    cloneroot = caseroot
    for i in range(int(startval), int(startval)+ensemble):
        member_string = '{{0:0{0:d}d}}'.format(nint).format(i)
        print("member_string="+member_string)
        if ensemble > 1:
            caseroot = caseroot[:-nint] + member_string
        if overwrite and os.path.isdir(caseroot):
            shutil.rmtree(caseroot)
        if not os.path.isdir(caseroot):
            with Case(cloneroot, read_only=False) as clone:
                clone.create_clone(caseroot, keepexe=True,
                                   user_mods_dir=user_mods_dir)
        with Case(caseroot, read_only=False) as case:
            per_run_case_updates(case, user_mods_dir, member_string, nint, paramdict)


def _main_func(description):

    print ("Starting SCAM PPE case creation, building, and submission script")
    print ("Base case name is {}".format(basecasename))
    print ("Usermods located in "+user_mods_dir)
    print ("Parameter file is "+paramfile)

    overwrite = True

    # read in NetCDF parameter file
    inptrs = Dataset(paramfile,'r')
    print ("Variables in paramfile:")
    print (inptrs.variables.keys())
    print ("Dimensions in paramfile:")
    print (inptrs.dimensions.keys())
    num_sims = inptrs.dimensions['nmb_sim'].size
    num_vars = len(inptrs.variables.keys())

    print ("Number of sims = {}".format(num_sims))
    print ("Number of params = {}".format(num_vars))

    # Save a pointer to the netcdf variables
    paramdict = inptrs.variables

    # Create and build the base case that all PPE cases are cloned from
    caseroot = build_base_case(baseroot, basecasename, res,
                            compset, overwrite, user_mods_dir)

    # Pass in a dictionary with all of the parameters and their values
    # for each PPE simulation 
    # This code clones the base case, using the same build as the base case,
    # Adds the namelist parameters from the paramfile to the user_nl_cam 
    # file of each new case, does a case.set up, builds namelists, and 
    # submits the runs.
    clone_base_case(caseroot, num_sims, user_mods_dir, overwrite, paramdict)

    inptrs.close()

if __name__ == "__main__":
    _main_func(__doc__)
