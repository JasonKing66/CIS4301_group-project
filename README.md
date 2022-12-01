# CIS4301_group-project
Used car sales web app 

Setup: This setup instruction is developed for Windows 10 only. 

1. Requirements: You will need to install latest Python (I used 3.10), Oracle 19c database, Pycharm IDE and original csv file from Kaggle (almost 10g one). Make sure you install and setup above mentioned tools, especially Oracle 19c Database. You need to setup and test it with SQL*Plus to make sure the connection is correct. Run some basic SQL commands to insert and display something. Oracle setup will take 30-60 minutes. If you can't get Oracle database configured correctly, don't move forward. 

2. Download both .py file to your local machine. Place the csv file on the same directory as both .py file. Open both files with Pycharm. Follow Pycharm recommendation to install all dependancies and libraries. Your Pycharm may take a while to read all missing dependencies. Wait for a minute or two. If you still can't see the installation recommendation, run 
pip install requirements.txt
If you still encounter errors suggest that missing some dependancies, consider manually pip install the missing dependancies based on error messages. 

3. Configure oracle connecction to your local Oracle database credentials. Namely the root, passwd, host, port and sid for both files. Files here contain credentials only work for my local database. It won't work for you. 

4. Run run_app.py first. This will import data into the database and generate desired dataset for app use. I use drop_key to delete many useless features but didn't touch the entrys. This process will take about 30-60 minutes. 

5. After you see "database imported. Application initialized", run New_web.py. Make sure New_web.py has updated oracle database credentials too.

6. If everything works well, you'll see the web app is up and running on http://127.0.0.1:7777/
