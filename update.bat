@echo off
cd /d "%~dp0"

echo ======================================================
echo  [1/2] Git Add and Commit
echo ======================================================
git add .
git commit -m "Update dashboard code and data (%date% %time%)"

echo.
echo ======================================================
echo  [2/2] Git Push to GitHub
echo ======================================================
git push origin main

echo.
echo ======================================================
echo  Deployment requested! 
echo  Render will update automatically in 1-2 minutes.
echo ======================================================
echo.
pause