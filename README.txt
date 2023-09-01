***  Installation ***

install python 3.8 for your operating sysytem using this link  https://www.python.org/downloads/

(optional)
install pycharm community edition using this link  https://www.jetbrains.com/pycharm/download/


*** Setup  ***

run following commands from command prompt/terminal

for MacOs/Linux:
      cd < project directory>           # move to project directory
      python -m venv venv              # create virtual environment
      source venv/bin/activate         # activate virtual environment
      pip install -r requirements.txt    # install dependencies

for windows:
       cd < project directory>           # move to project directory
       python -m venv venv              # create virtual environment
       venv\Scripts\activate            # activate virtual environment
       pip install -r requirements.txt     # install dependencies
       if any error during installation run this command:
       pip install Twisted-20.3.0-cp38-cp38-win_amd64.whl and the run: pip install -r requirements.txt again


*** Kite Credentials ***

provide your zerodha credential inside kite_config.json file in config folder if not provided already


*** Parameters ***

provide your parameters in parameters.xlsx file as instructed in sheet 2


*** How To Run ***

Option 1:
    To run using pycharm:

    set run.py located in main folder in pycharm configuration and click on run button to run program

Option 2:
    To run using terminal:

    run following commands from command prompt/terminal

    for MacOs/Linux:
          cd < project directory>           # move to project directory
          source venv/bin/activate         # activate virtual environment
          python run.py   # Run program

    for windows:
           cd < project directory>           # move to project directory
           venv\Scripts\activate            # activate virtual environment
           run.py           # Run program

Option 3:
    for windows OS only:
    double click run.bat file



*** metrics ***

each trade will be stored in database and you can run metrics.py which will generate trade_results.xlsx file,
which will contain records of all closed positions,
run process is same as specified above just replace run.py/run.bat with metrics.py/metrics.bat



*** logs ***

Each run will create date wise log files inside logs folder showing all details of trading