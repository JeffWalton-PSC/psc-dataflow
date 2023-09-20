"""
parameters used by Starfish flows and tasks
"""
from pathlib import WindowsPath

BEGIN_YEAR = "2011"
CATALOG_YEAR = "2023"
N_YEARS_ACTIVE_WINDOW = 8

starfish_files_path = WindowsPath(r"F:\Applications\Starfish\Files")
starfish_workingfiles_path = starfish_files_path / "workingfiles"
starfish_prod_sisdatafiles_path = starfish_files_path / "prod\sisdatafiles"
starfish_test_sisdatafiles_path = starfish_files_path / "test\sisdatafiles"
